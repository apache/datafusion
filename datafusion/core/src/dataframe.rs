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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::compute::{cast, concat};
use arrow::csv::WriterBuilder;
use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_common::file_options::parquet_writer::{
    default_builder, ParquetWriterOptions,
};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    DataFusionError, FileType, FileTypeWriterOptions, SchemaError, UnnestOptions,
};
use datafusion_expr::dml::CopyOptions;
use parquet::file::properties::WriterProperties;

use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::{
    avg, count, is_null, max, median, min, stddev, utils::COUNT_STAR_EXPANSION,
    TableProviderFilterPushDown, UNNAMED_TABLE,
};

use crate::arrow::datatypes::Schema;
use crate::arrow::datatypes::SchemaRef;
use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::pretty;
use crate::datasource::{provider_as_source, MemTable, TableProvider};
use crate::error::Result;
use crate::execution::{
    context::{SessionState, TaskContext},
    FunctionRegistry,
};
use crate::logical_expr::{
    col, utils::find_window_exprs, Expr, JoinType, LogicalPlan, LogicalPlanBuilder,
    Partitioning, TableType,
};
use crate::physical_plan::SendableRecordBatchStream;
use crate::physical_plan::{collect, collect_partitioned};
use crate::physical_plan::{execute_stream, execute_stream_partitioned, ExecutionPlan};
use crate::prelude::SessionContext;

/// Contains options that control how data is
/// written out from a DataFrame
pub struct DataFrameWriteOptions {
    /// Controls if existing data should be overwritten
    overwrite: bool,
    /// Controls if all partitions should be coalesced into a single output file
    /// Generally will have slower performance when set to true.
    single_file_output: bool,
    /// Sets compression by DataFusion applied after file serialization.
    /// Allows compression of CSV and JSON.
    /// Not supported for parquet.
    compression: CompressionTypeVariant,
}

impl DataFrameWriteOptions {
    /// Create a new DataFrameWriteOptions with default values
    pub fn new() -> Self {
        DataFrameWriteOptions {
            overwrite: false,
            single_file_output: false,
            compression: CompressionTypeVariant::UNCOMPRESSED,
        }
    }
    /// Set the overwrite option to true or false
    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Set the single_file_output value to true or false
    pub fn with_single_file_output(mut self, single_file_output: bool) -> Self {
        self.single_file_output = single_file_output;
        self
    }

    /// Sets the compression type applied to the output file(s)
    pub fn with_compression(mut self, compression: CompressionTypeVariant) -> Self {
        self.compression = compression;
        self
    }
}

impl Default for DataFrameWriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

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
/// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(0, Some(100))?;
/// let results = df.collect();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DataFrame {
    session_state: SessionState,
    plan: LogicalPlan,
}

impl DataFrame {
    /// Create a new Table based on an existing logical plan
    pub fn new(session_state: SessionState, plan: LogicalPlan) -> Self {
        Self {
            session_state,
            plan,
        }
    }

    /// Create a physical plan
    pub async fn create_physical_plan(self) -> Result<Arc<dyn ExecutionPlan>> {
        self.session_state.create_physical_plan(&self.plan).await
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.select_columns(&["a", "b"])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn select_columns(self, columns: &[&str]) -> Result<DataFrame> {
        let fields = columns
            .iter()
            .map(|name| self.plan.schema().field_with_unqualified_name(name))
            .collect::<Result<Vec<_>>>()?;
        let expr: Vec<Expr> = fields
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.select(vec![col("a") * col("b"), col("c")])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn select(self, expr_list: Vec<Expr>) -> Result<DataFrame> {
        let window_func_exprs = find_window_exprs(&expr_list);
        let plan = if window_func_exprs.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::window_plan(self.plan, window_func_exprs)?
        };
        let project_plan = LogicalPlanBuilder::from(plan).project(expr_list)?.build()?;

        Ok(DataFrame::new(self.session_state, project_plan))
    }

    /// Expand each list element of a column to multiple rows.
    ///
    /// Seee also:
    ///
    /// 1. [`UnnestOptions`] documentation for the behavior of `unnest`
    /// 2. [`Self::unnest_column_with_options`]
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.unnest_column("a")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn unnest_column(self, column: &str) -> Result<DataFrame> {
        self.unnest_column_with_options(column, UnnestOptions::new())
    }

    /// Expand each list element of a column to multiple rows, with
    /// behavior controlled by [`UnnestOptions`].
    ///
    /// Please see the documentation on [`UnnestOptions`] for more
    /// details about the meaning of unnest.
    pub fn unnest_column_with_options(
        self,
        column: &str,
        options: UnnestOptions,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .unnest_column_with_options(column, options)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Filter a DataFrame to only include rows that match the specified filter expression.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.filter(col("a").lt_eq(col("b")))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter(self, predicate: Expr) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .filter(predicate)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Perform an aggregate query with optional grouping expressions.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b) GROUP BY a"
    /// let _ = df.clone().aggregate(vec![col("a")], vec![min(col("b"))])?;
    ///
    /// // The following use is the equivalent of "SELECT MIN(b)"
    /// let _ = df.aggregate(vec![], vec![min(col("b"))])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn aggregate(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Apply one or more window functions ([`Expr::WindowFunction`]) to extend the schema
    pub fn window(self, window_exprs: Vec<Expr>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .window(window_exprs)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.limit(0, Some(100))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .limit(skip, fetch)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let d2 = df.clone();
    /// let df = df.union(d2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn union(self, dataframe: DataFrame) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .union(dataframe.plan)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let d2 = df.clone();
    /// let df = df.union_distinct(d2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn union_distinct(self, dataframe: DataFrame) -> Result<DataFrame> {
        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::from(self.plan)
                .union_distinct(dataframe.plan)?
                .build()?,
        ))
    }

    /// Filter out duplicate rows
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.distinct()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn distinct(self) -> Result<DataFrame> {
        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::from(self.plan).distinct()?.build()?,
        ))
    }

    /// Summary statistics for a DataFrame. Only summarizes numeric datatypes at the moment and
    /// returns nulls for non numeric datatypes. Try in keep output similar to pandas
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use arrow::util::pretty;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/tpch-csv/customer.csv", CsvReadOptions::new()).await?;
    /// df.describe().await.unwrap();
    ///
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
        let describe_record_batch = vec![
            // count aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| count(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // null_count aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .map(|f| count(is_null(col(f.name()))).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // mean aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| avg(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // std aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| stddev(col(f.name())).alias(f.name()))
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
                    .map(|f| min(col(f.name())).alias(f.name()))
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
                    .map(|f| max(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            ),
            // median aggregation
            self.clone().aggregate(
                vec![],
                original_schema_fields
                    .clone()
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| median(col(f.name())).alias(f.name()))
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
                        let batchs = df.clone().collect().await;
                        match batchs {
                            Ok(batchs)
                                if batchs.len() == 1
                                    && batchs[0]
                                        .column_by_name(field.name())
                                        .is_some() =>
                            {
                                let column =
                                    batchs[0].column_by_name(field.name()).unwrap();
                                if field.data_type().is_numeric() {
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
                    Err(other_err) => {
                        panic!("{other_err}")
                    }
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
        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::scan(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        ))
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.sort(vec![col("a").sort(true, true), col("b").sort(false, false)])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sort(self, expr: Vec<Expr>) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan).sort(expr)?.build()?;
        Ok(DataFrame::new(self.session_state, plan))
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
    /// let left = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let right = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?
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
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Join this DataFrame with another DataFrame using the specified expressions.
    ///
    /// Simply a thin wrapper over [`join`](Self::join) where the join keys are not provided,
    /// and the provided expressions are AND'ed together to form the filter expression.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
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
    /// let join_on = left.join_on(
    ///     right,
    ///     JoinType::Inner,
    ///     [col("a").not_eq(col("a2")), col("b").not_eq(col("b2"))],
    /// )?;
    /// let batches = join_on.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn join_on(
        self,
        right: DataFrame,
        join_type: JoinType,
        on_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<DataFrame> {
        let expr = on_exprs.into_iter().reduce(Expr::and);
        let plan = LogicalPlanBuilder::from(self.plan)
            .join(
                right.plan,
                join_type,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                expr,
            )?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Repartition a DataFrame based on a logical partitioning scheme.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df1 = df.repartition(Partitioning::RoundRobinBatch(4))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn repartition(self, partitioning_scheme: Partitioning) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .repartition(partitioning_scheme)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Run a count aggregate on the DataFrame and execute the DataFrame to collect this
    /// count and return it as a usize, to find the total number of rows after executing
    /// the DataFrame.
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let count = df.count().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count(self) -> Result<usize> {
        let rows = self
            .aggregate(
                vec![],
                vec![datafusion_expr::count(Expr::Literal(COUNT_STAR_EXPANSION))],
            )?
            .collect()
            .await?;
        let len = *rows
            .first()
            .and_then(|r| r.columns().first())
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .and_then(|a| a.values().first())
            .ok_or(DataFusionError::Internal(
                "Unexpected output when collecting for count()".to_string(),
            ))? as usize;
        Ok(len)
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn collect(self) -> Result<Vec<RecordBatch>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// df.show().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show(self) -> Result<()> {
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// df.show_limit(10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_limit(self, num: usize) -> Result<()> {
        let results = self.limit(0, Some(num))?.collect().await?;
        Ok(pretty::print_batches(&results)?)
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> TaskContext {
        TaskContext::from(&self.session_state)
    }

    /// Executes this DataFrame and returns a stream over a single partition
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let stream = df.execute_stream().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_stream(self) -> Result<SendableRecordBatchStream> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        execute_stream(plan, task_ctx)
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
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
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.execute_stream_partitioned().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_stream_partitioned(
        self,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let task_ctx = Arc::new(self.task_ctx());
        let plan = self.create_physical_plan().await?;
        execute_stream_partitioned(plan, task_ctx)
    }

    /// Returns the schema describing the output of this DataFrame in terms of columns returned,
    /// where each column has a name, data type, and nullability attribute.

    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let schema = df.schema();
    /// # Ok(())
    /// # }
    /// ```
    pub fn schema(&self) -> &DFSchema {
        self.plan.schema()
    }

    /// Return the unoptimized logical plan
    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.plan
    }

    /// Returns both the [`LogicalPlan`] and [`SessionState`] that comprise this [`DataFrame`]
    pub fn into_parts(self) -> (SessionState, LogicalPlan) {
        (self.session_state, self.plan)
    }

    /// Return the logical plan represented by this DataFrame without running the optimizers
    ///
    /// Note: This method should not be used outside testing, as it loses the snapshot
    /// of the [`SessionState`] attached to this [`DataFrame`] and consequently subsequent
    /// operations may take place against a different state
    pub fn into_unoptimized_plan(self) -> LogicalPlan {
        self.plan
    }

    /// Return the optimized logical plan represented by this DataFrame.
    ///
    /// Note: This method should not be used outside testing, as it loses the snapshot
    /// of the [`SessionState`] attached to this [`DataFrame`] and consequently subsequent
    /// operations may take place against a different state
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
        Arc::new(DataFrameTableProvider { plan: self.plan })
    }

    /// Return the optimized logical plan represented by this DataFrame.
    ///
    /// Note: This method should not be used outside testing, as it loses the snapshot
    /// of the [`SessionState`] attached to this [`DataFrame`] and consequently subsequent
    /// operations may take place against a different state
    #[deprecated(since = "23.0.0", note = "Use DataFrame::into_optimized_plan")]
    pub fn to_logical_plan(self) -> Result<LogicalPlan> {
        self.into_optimized_plan()
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let batches = df.limit(0, Some(100))?.explain(false, false)?.collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn explain(self, verbose: bool, analyze: bool) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .explain(verbose, analyze)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }

    /// Return a `FunctionRegistry` used to plan udf's calls
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let f = df.registry();
    /// // use f.udf("name", vec![...]) to use the udf
    /// # Ok(())
    /// # }
    /// ```
    pub fn registry(&self) -> &dyn FunctionRegistry {
        &self.session_state
    }

    /// Calculate the intersection of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let d2 = df.clone();
    /// let df = df.intersect(d2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn intersect(self, dataframe: DataFrame) -> Result<DataFrame> {
        let left_plan = self.plan;
        let right_plan = dataframe.plan;
        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::intersect(left_plan, right_plan, true)?,
        ))
    }

    /// Calculate the exception of two [`DataFrame`]s.  The two [`DataFrame`]s must have exactly the same schema
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let d2 = df.clone();
    /// let df = df.except(d2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn except(self, dataframe: DataFrame) -> Result<DataFrame> {
        let left_plan = self.plan;
        let right_plan = dataframe.plan;

        Ok(DataFrame::new(
            self.session_state,
            LogicalPlanBuilder::except(left_plan, right_plan, true)?,
        ))
    }

    /// Write this DataFrame to the referenced table
    /// This method uses on the same underlying implementation
    /// as the SQL Insert Into statement.
    /// Unlike most other DataFrame methods, this method executes
    /// eagerly, writing data, and returning the count of rows written.
    pub async fn write_table(
        self,
        table_name: &str,
        write_options: DataFrameWriteOptions,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let arrow_schema = Schema::from(self.schema());
        let plan = LogicalPlanBuilder::insert_into(
            self.plan,
            table_name.to_owned(),
            &arrow_schema,
            write_options.overwrite,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }

    /// Write a `DataFrame` to a CSV file.
    pub async fn write_csv(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_properties: Option<WriterBuilder>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented for DataFrame::write_csv.".to_owned(),
            ));
        }
        let props = match writer_properties {
            Some(props) => props,
            None => WriterBuilder::new(),
        };

        let file_type_writer_options =
            FileTypeWriterOptions::CSV(CsvWriterOptions::new(props, options.compression));
        let copy_options = CopyOptions::WriterOptions(Box::new(file_type_writer_options));

        let plan = LogicalPlanBuilder::copy_to(
            self.plan,
            path.into(),
            FileType::CSV,
            options.single_file_output,
            copy_options,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }

    /// Write a `DataFrame` to a Parquet file.
    pub async fn write_parquet(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_properties: Option<WriterProperties>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented for DataFrame::write_parquet.".to_owned(),
            ));
        }
        match options.compression{
            CompressionTypeVariant::UNCOMPRESSED => (),
            _ => return Err(DataFusionError::Configuration("DataFrame::write_parquet method does not support compression set via DataFrameWriteOptions. Set parquet compression via writer_properties instead.".to_owned()))
        }
        let props = match writer_properties {
            Some(props) => props,
            None => default_builder(self.session_state.config_options())?.build(),
        };
        let file_type_writer_options =
            FileTypeWriterOptions::Parquet(ParquetWriterOptions::new(props));
        let copy_options = CopyOptions::WriterOptions(Box::new(file_type_writer_options));
        let plan = LogicalPlanBuilder::copy_to(
            self.plan,
            path.into(),
            FileType::PARQUET,
            options.single_file_output,
            copy_options,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    pub async fn write_json(
        self,
        path: &str,
        options: DataFrameWriteOptions,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented for DataFrame::write_json.".to_owned(),
            ));
        }
        let file_type_writer_options =
            FileTypeWriterOptions::JSON(JsonWriterOptions::new(options.compression));
        let copy_options = CopyOptions::WriterOptions(Box::new(file_type_writer_options));
        let plan = LogicalPlanBuilder::copy_to(
            self.plan,
            path.into(),
            FileType::JSON,
            options.single_file_output,
            copy_options,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }

    /// Add an additional column to the DataFrame.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.with_column("ab_sum", col("a") + col("b"))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_column(self, name: &str, expr: Expr) -> Result<DataFrame> {
        let window_func_exprs = find_window_exprs(&[expr.clone()]);
        let plan = if window_func_exprs.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::window_plan(self.plan, window_func_exprs)?
        };

        let new_column = expr.alias(name);
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
                    Expr::Column(Column {
                        relation: None,
                        name: f.name().into(),
                    })
                }
            })
            .collect();

        if !col_exists {
            fields.push(new_column);
        }

        let project_plan = LogicalPlanBuilder::from(plan).project(fields)?.build()?;

        Ok(DataFrame::new(self.session_state, project_plan))
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
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.with_column_renamed("ab_sum", "total")?;
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

        let field_to_rename = match self.plan.schema().field_from_column(&old_column) {
            Ok(field) => field,
            // no-op if field not found
            Err(DataFusionError::SchemaError(SchemaError::FieldNotFound { .. })) => {
                return Ok(self)
            }
            Err(err) => return Err(err),
        };
        let projection = self
            .plan
            .schema()
            .fields()
            .iter()
            .map(|f| {
                if f == field_to_rename {
                    col(f.qualified_column()).alias(new_name)
                } else {
                    col(f.qualified_column())
                }
            })
            .collect::<Vec<_>>();
        let project_plan = LogicalPlanBuilder::from(self.plan)
            .project(projection)?
            .build()?;
        Ok(DataFrame::new(self.session_state, project_plan))
    }

    /// Convert a prepare logical plan into its inner logical plan with all params replaced with their corresponding values
    pub fn with_param_values(self, param_values: Vec<ScalarValue>) -> Result<Self> {
        let plan = self.plan.with_param_values(param_values)?;
        Ok(Self::new(self.session_state, plan))
    }

    /// Cache DataFrame as a memory table.
    ///
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let ctx = SessionContext::new();
    /// let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
    /// let df = df.cache().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cache(self) -> Result<DataFrame> {
        let context = SessionContext::with_state(self.session_state.clone());
        let mem_table = MemTable::try_new(
            SchemaRef::from(self.schema().clone()),
            self.collect_partitioned().await?,
        )?;

        context.read_table(Arc::new(mem_table))
    }
}

struct DataFrameTableProvider {
    plan: LogicalPlan,
}

#[async_trait]
impl TableProvider for DataFrameTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        Some(&self.plan)
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        // A filter is added on the DataFrame when given
        Ok(TableProviderFilterPushDown::Exact)
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut expr = LogicalPlanBuilder::from(self.plan.clone());
        if let Some(p) = projection {
            expr = expr.select(p.iter().copied())?
        }

        // Add filter when given
        let filter = filters.iter().cloned().reduce(|acc, new| acc.and(new));
        if let Some(filter) = filter {
            expr = expr.filter(filter)?
        }
        // add a limit if given
        if let Some(l) = limit {
            expr = expr.limit(0, Some(l))?
        }
        let plan = expr.build()?;
        state.create_physical_plan(&plan).await
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use arrow::array::Int32Array;
    use arrow::datatypes::DataType;

    use datafusion_expr::{
        avg, cast, count, count_distinct, create_udf, expr, lit, max, min, sum,
        BuiltInWindowFunction, ScalarFunctionImplementation, Volatility, WindowFrame,
        WindowFunction,
    };
    use datafusion_physical_expr::expressions::Column;
    use object_store::local::LocalFileSystem;
    use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
    use parquet::file::reader::FileReader;
    use tempfile::TempDir;
    use url::Url;

    use crate::execution::context::SessionConfig;
    use crate::execution::options::{CsvReadOptions, ParquetReadOptions};
    use crate::physical_plan::ColumnarValue;
    use crate::physical_plan::Partitioning;
    use crate::physical_plan::PhysicalExpr;
    use crate::test_util;
    use crate::test_util::parquet_test_data;
    use crate::{assert_batches_sorted_eq, execution::context::SessionContext};

    use super::*;

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
        let first_row = Expr::WindowFunction(expr::WindowFunction::new(
            WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::FirstValue),
            vec![col("aggregate_test_100.c1")],
            vec![col("aggregate_test_100.c2")],
            vec![],
            WindowFrame::new(false),
        ));
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
    async fn select_with_periods() -> Result<()> {
        // define data with a column name that has a "." in it:
        let array: Int32Array = [1, 10].into_iter().collect();
        let batch = RecordBatch::try_from_iter(vec![("f.c1", Arc::new(array) as _)])?;

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch)?;

        let df = ctx.table("t").await?.select_columns(&["f.c1"])?;

        let df_results = df.collect().await?;

        assert_batches_sorted_eq!(
            ["+------+", "| f.c1 |", "+------+", "| 1    |", "| 10   |", "+------+"],
            &df_results
        );

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
            ["+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| c1 | MIN(aggregate_test_100.c12) | MAX(aggregate_test_100.c12) | AVG(aggregate_test_100.c12) | SUM(aggregate_test_100.c12) | COUNT(aggregate_test_100.c12) | COUNT(DISTINCT aggregate_test_100.c12) |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+",
                "| a  | 0.02182578039211991         | 0.9800193410444061          | 0.48754517466109415         | 10.238448667882977          | 21                            | 21                                     |",
                "| b  | 0.04893135681998029         | 0.9185813970744787          | 0.41040709263815384         | 7.797734760124923           | 19                            | 19                                     |",
                "| c  | 0.0494924465469434          | 0.991517828651004           | 0.6600456536439784          | 13.860958726523545          | 21                            | 21                                     |",
                "| d  | 0.061029375346466685        | 0.9748360509016578          | 0.48855379387549824         | 8.793968289758968           | 18                            | 18                                     |",
                "| e  | 0.01479305307777301         | 0.9965400387585364          | 0.48600669271341534         | 10.206140546981722          | 21                            | 21                                     |",
                "+----+-----------------------------+-----------------------------+-----------------------------+-----------------------------+-------------------------------+----------------------------------------+"],
            &df
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_distinct() -> Result<()> {
        let t = test_table().await?;
        let plan = t
            .select(vec![col("c1")])
            .unwrap()
            .distinct()
            .unwrap()
            .plan
            .clone();

        let sql_plan = create_plan("select distinct c1 from aggregate_test_100").await?;

        assert_same_plan(&plan, &sql_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_distinct_sort_by() -> Result<()> {
        let t = test_table().await?;
        let plan = t
            .select(vec![col("c1")])
            .unwrap()
            .distinct()
            .unwrap()
            .sort(vec![col("c1").sort(true, true)])
            .unwrap();

        let df_results = plan.clone().collect().await?;

        #[rustfmt::skip]
        assert_batches_sorted_eq!(
            ["+----+",
                "| c1 |",
                "+----+",
                "| a  |",
                "| b  |",
                "| c  |",
                "| d  |",
                "| e  |",
                "+----+"],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_distinct_sort_by_unprojected() -> Result<()> {
        let t = test_table().await?;
        let err = t
            .select(vec![col("c1")])
            .unwrap()
            .distinct()
            .unwrap()
            // try to sort on some value not present in input to distinct
            .sort(vec![col("c2").sort(true, true)])
            .unwrap_err();
        assert_eq!(err.to_string(), "Error during planning: For SELECT DISTINCT, ORDER BY expressions c2 must appear in select list");

        Ok(())
    }

    #[tokio::test]
    async fn join() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c3"])?;
        let left_rows = left.clone().collect().await?;
        let right_rows = right.clone().collect().await?;
        let join = left.join(right, JoinType::Inner, &["c1"], &["c1"], None)?;
        let join_rows = join.collect().await?;
        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(2008, join_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        Ok(())
    }

    #[tokio::test]
    async fn join_on() -> Result<()> {
        let left = test_table_with_name("a")
            .await?
            .select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("b")
            .await?
            .select_columns(&["c1", "c2"])?;
        let join = left.join_on(
            right,
            JoinType::Inner,
            [col("a.c1").not_eq(col("b.c1")), col("a.c2").eq(col("b.c2"))],
        )?;

        let expected_plan = "Inner Join:  Filter: a.c1 != b.c1 AND a.c2 = b.c2\
        \n  Projection: a.c1, a.c2\
        \n    TableScan: a\
        \n  Projection: b.c1, b.c2\
        \n    TableScan: b";
        assert_eq!(expected_plan, format!("{:?}", join.logical_plan()));

        Ok(())
    }

    #[tokio::test]
    async fn join_ambiguous_filter() -> Result<()> {
        let left = test_table_with_name("a")
            .await?
            .select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("b")
            .await?
            .select_columns(&["c1", "c2"])?;

        let join = left
            .join_on(right, JoinType::Inner, [col("c1").eq(col("c1"))])
            .expect_err("join didn't fail check");
        let expected = "Schema error: Ambiguous reference to unqualified field c1";
        assert_eq!(join.to_string(), expected);

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
    async fn df_count() -> Result<()> {
        let count = test_table().await?.count().await?;
        assert_eq!(100, count);
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
        let df = ctx.table("aggregate_test_100").await?;

        let expr = df.registry().udf("my_fn")?.call(vec![col("c12")]);
        let df = df.select(vec![expr])?;

        // build query using SQL
        let sql_plan = ctx.sql("SELECT my_fn(c12) FROM aggregate_test_100").await?;

        // the two plans should be identical
        assert_same_plan(&df.plan, &sql_plan.plan);

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
        let d2 = df.clone();
        let plan = df.intersect(d2)?;
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
        let d2 = df.clone();
        let plan = df.except(d2)?;
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
        let df_impl = DataFrame::new(ctx.state(), df.plan.clone());

        // register a dataframe as a table
        ctx.register_table("test_table", df_impl.clone().into_view())?;

        // pull the table out
        let table = ctx.table("test_table").await?;

        let group_expr = vec![col("c1")];
        let aggr_expr = vec![sum(col("c12"))];

        // check that we correctly read from the table
        let df_results = df_impl
            .aggregate(group_expr.clone(), aggr_expr.clone())?
            .collect()
            .await?;
        let table_results = &table.aggregate(group_expr, aggr_expr)?.collect().await?;

        assert_batches_sorted_eq!(
            [
                "+----+-----------------------------+",
                "| c1 | SUM(aggregate_test_100.c12) |",
                "+----+-----------------------------+",
                "| a  | 10.238448667882977          |",
                "| b  | 7.797734760124923           |",
                "| c  | 13.860958726523545          |",
                "| d  | 8.793968289758968           |",
                "| e  | 10.206140546981722          |",
                "+----+-----------------------------+"
            ],
            &df_results
        );

        // the results are the same as the results from the view, modulo the leaf table name
        assert_batches_sorted_eq!(
            [
                "+----+---------------------+",
                "| c1 | SUM(test_table.c12) |",
                "+----+---------------------+",
                "| a  | 10.238448667882977  |",
                "| b  | 7.797734760124923   |",
                "| c  | 13.860958726523545  |",
                "| d  | 8.793968289758968   |",
                "| e  | 10.206140546981722  |",
                "+----+---------------------+"
            ],
            table_results
        );
        Ok(())
    }

    /// Compare the formatted string representation of two plans for equality
    fn assert_same_plan(plan1: &LogicalPlan, plan2: &LogicalPlan) {
        assert_eq!(format!("{plan1:?}"), format!("{plan2:?}"));
    }

    /// Create a logical plan from a SQL query
    async fn create_plan(sql: &str) -> Result<LogicalPlan> {
        let mut ctx = SessionContext::new();
        register_aggregate_csv(&mut ctx, "aggregate_test_100").await?;
        Ok(ctx.sql(sql).await?.into_unoptimized_plan())
    }

    async fn test_table_with_name(name: &str) -> Result<DataFrame> {
        let mut ctx = SessionContext::new();
        register_aggregate_csv(&mut ctx, name).await?;
        ctx.table(name).await
    }

    async fn test_table() -> Result<DataFrame> {
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
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            CsvReadOptions::new().schema(schema.as_ref()),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn with_column() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
        let ctx = SessionContext::new();
        let df_impl = DataFrame::new(ctx.state(), df.plan.clone());

        let df = df_impl
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .with_column("sum", col("c2") + col("c3"))?;

        // check that new column added
        let df_results = df.clone().collect().await?;

        assert_batches_sorted_eq!(
            [
                "+----+----+-----+-----+",
                "| c1 | c2 | c3  | sum |",
                "+----+----+-----+-----+",
                "| a  | 3  | -12 | -9  |",
                "| a  | 3  | -72 | -69 |",
                "| a  | 3  | 13  | 16  |",
                "| a  | 3  | 13  | 16  |",
                "| a  | 3  | 14  | 17  |",
                "| a  | 3  | 17  | 20  |",
                "+----+----+-----+-----+"
            ],
            &df_results
        );

        // check that col with the same name ovwewritten
        let df_results_overwrite = df
            .clone()
            .with_column("c1", col("c2") + col("c3"))?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            [
                "+-----+----+-----+-----+",
                "| c1  | c2 | c3  | sum |",
                "+-----+----+-----+-----+",
                "| -69 | 3  | -72 | -69 |",
                "| -9  | 3  | -12 | -9  |",
                "| 16  | 3  | 13  | 16  |",
                "| 16  | 3  | 13  | 16  |",
                "| 17  | 3  | 14  | 17  |",
                "| 20  | 3  | 17  | 20  |",
                "+-----+----+-----+-----+"
            ],
            &df_results_overwrite
        );

        // check that col with the same name ovwewritten using same name as reference
        let df_results_overwrite_self = df
            .clone()
            .with_column("c2", col("c2") + lit(1))?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            [
                "+----+----+-----+-----+",
                "| c1 | c2 | c3  | sum |",
                "+----+----+-----+-----+",
                "| a  | 4  | -12 | -9  |",
                "| a  | 4  | -72 | -69 |",
                "| a  | 4  | 13  | 16  |",
                "| a  | 4  | 13  | 16  |",
                "| a  | 4  | 14  | 17  |",
                "| a  | 4  | 17  | 20  |",
                "+----+----+-----+-----+"
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

        let df_sum_renamed = df
            .with_column_renamed("sum", "total")?
            // table qualifier optional
            .with_column_renamed("c1", "one")?
            // accepts table qualifier
            .with_column_renamed("aggregate_test_100.c2", "two")?
            // no-op for missing column
            .with_column_renamed("c4", "boom")?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            [
                "+-----+-----+----+-------+",
                "| one | two | c3 | total |",
                "+-----+-----+----+-------+",
                "| a   | 3   | 13 | 16    |",
                "+-----+-----+----+-------+"
            ],
            &df_sum_renamed
        );

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed_ambiguous() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
        let ctx = SessionContext::new();

        let table = df.into_view();
        ctx.register_table("t1", table.clone())?;
        ctx.register_table("t2", table)?;

        let actual_err = ctx
            .table("t1")
            .await?
            .join(
                ctx.table("t2").await?,
                JoinType::Inner,
                &["c1"],
                &["c1"],
                None,
            )?
            // can be t1.c2 or t2.c2
            .with_column_renamed("c2", "AAA")
            .unwrap_err();
        let expected_err = "Schema error: Ambiguous reference to unqualified field c2";
        assert_eq!(actual_err.to_string(), expected_err);

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed_join() -> Result<()> {
        let df = test_table().await?.select_columns(&["c1", "c2", "c3"])?;
        let ctx = SessionContext::new();

        let table = df.into_view();
        ctx.register_table("t1", table.clone())?;
        ctx.register_table("t2", table)?;
        let df = ctx
            .table("t1")
            .await?
            .join(
                ctx.table("t2").await?,
                JoinType::Inner,
                &["c1"],
                &["c1"],
                None,
            )?
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

        let df_results = df.clone().collect().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+-----+----+----+-----+",
                "| c1 | c2 | c3  | c1 | c2 | c3  |",
                "+----+----+-----+----+----+-----+",
                "| a  | 1  | -85 | a  | 1  | -85 |",
                "+----+----+-----+----+----+-----+"
            ],
            &df_results
        );

        let df_renamed = df.clone().with_column_renamed("t1.c1", "AAA")?;

        assert_eq!("\
        Projection: t1.c1 AS AAA, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3\
        \n  Limit: skip=0, fetch=1\
        \n    Sort: t1.c1 ASC NULLS FIRST, t1.c2 ASC NULLS FIRST, t1.c3 ASC NULLS FIRST, t2.c1 ASC NULLS FIRST, t2.c2 ASC NULLS FIRST, t2.c3 ASC NULLS FIRST\
        \n      Inner Join: t1.c1 = t2.c1\
        \n        TableScan: t1\
        \n        TableScan: t2",
                   format!("{:?}", df_renamed.logical_plan())
        );

        assert_eq!("\
        Projection: t1.c1 AS AAA, t1.c2, t1.c3, t2.c1, t2.c2, t2.c3\
        \n  Limit: skip=0, fetch=1\
        \n    Sort: t1.c1 ASC NULLS FIRST, t1.c2 ASC NULLS FIRST, t1.c3 ASC NULLS FIRST, t2.c1 ASC NULLS FIRST, t2.c2 ASC NULLS FIRST, t2.c3 ASC NULLS FIRST, fetch=1\
        \n      Inner Join: t1.c1 = t2.c1\
        \n        SubqueryAlias: t1\
        \n          TableScan: aggregate_test_100 projection=[c1, c2, c3]\
        \n        SubqueryAlias: t2\
        \n          TableScan: aggregate_test_100 projection=[c1, c2, c3]",
                   format!("{:?}", df_renamed.clone().into_optimized_plan()?)
        );

        let df_results = df_renamed.collect().await?;

        assert_batches_sorted_eq!(
            [
                "+-----+----+-----+----+----+-----+",
                "| AAA | c2 | c3  | c1 | c2 | c3  |",
                "+-----+----+-----+----+----+-----+",
                "| a   | 1  | -85 | a  | 1  | -85 |",
                "+-----+----+-----+----+----+-----+"
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn with_column_renamed_case_sensitive() -> Result<()> {
        let config =
            SessionConfig::from_string_hash_map(std::collections::HashMap::from([(
                "datafusion.sql_parser.enable_ident_normalization".to_owned(),
                "false".to_owned(),
            )]))?;
        let mut ctx = SessionContext::with_config(config);
        let name = "aggregate_test_100";
        register_aggregate_csv(&mut ctx, name).await?;
        let df = ctx.table(name);

        let df = df
            .await?
            .filter(col("c2").eq(lit(3)).and(col("c1").eq(lit("a"))))?
            .limit(0, Some(1))?
            .sort(vec![
                // make the test deterministic
                col("c1").sort(true, true),
                col("c2").sort(true, true),
                col("c3").sort(true, true),
            ])?
            .select_columns(&["c1"])?;

        let df_renamed = df.clone().with_column_renamed("c1", "CoLuMn1")?;

        let res = &df_renamed.clone().collect().await?;

        assert_batches_sorted_eq!(
            [
                "+---------+",
                "| CoLuMn1 |",
                "+---------+",
                "| a       |",
                "+---------+"
            ],
            res
        );

        let df_renamed = df_renamed
            .with_column_renamed("CoLuMn1", "c1")?
            .collect()
            .await?;

        assert_batches_sorted_eq!(
            ["+----+", "| c1 |", "+----+", "| a  |", "+----+"],
            &df_renamed
        );

        Ok(())
    }

    #[tokio::test]
    async fn filter_pushdown_dataframe() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_parquet(
            "test",
            &format!("{}/alltypes_plain.snappy.parquet", parquet_test_data()),
            ParquetReadOptions::default(),
        )
        .await?;

        ctx.register_table("t1", ctx.table("test").await?.into_view())?;

        let df = ctx
            .table("t1")
            .await?
            .filter(col("id").eq(lit(1)))?
            .select_columns(&["bool_col", "int_col"])?;

        let plan = df.explain(false, false)?.collect().await?;
        // Filters all the way to Parquet
        let formatted = pretty::pretty_format_batches(&plan)?.to_string();
        assert!(formatted.contains("FilterExec: id@0 = 1"));

        Ok(())
    }

    #[tokio::test]
    async fn cast_expr_test() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(1))?
            .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

        let df_results = df.clone().collect().await?;
        df.clone().show().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+-----+",
                "| c2 | c3 | sum |",
                "+----+----+-----+",
                "| 2  | 1  | 3   |",
                "+----+----+-----+"
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
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("2a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
                    Some("3a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000800"),
                ]))
            ],
        )?;

        let ctx = SessionContext::new();
        ctx.register_batch("test", data)?;

        let sql = r#"
        SELECT
            COUNT(1)
        FROM
            test
        GROUP BY
            column_1"#;

        let df = ctx.sql(sql).await?;
        df.show_limit(10).await?;

        Ok(())
    }

    #[tokio::test]
    async fn with_column_name() -> Result<()> {
        // define data with a column name that has a "." in it:
        let array: Int32Array = [1, 10].into_iter().collect();
        let batch = RecordBatch::try_from_iter(vec![("f.c1", Arc::new(array) as _)])?;

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch)?;

        let df = ctx
            .table("t")
            .await?
            // try and create a column with a '.' in it
            .with_column("f.c2", lit("hello"))?;

        let df_results = df.collect().await?;

        assert_batches_sorted_eq!(
            [
                "+------+-------+",
                "| f.c1 | f.c2  |",
                "+------+-------+",
                "| 1    | hello |",
                "| 10   | hello |",
                "+------+-------+"
            ],
            &df_results
        );

        Ok(())
    }

    #[tokio::test]
    async fn cache_test() -> Result<()> {
        let df = test_table()
            .await?
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(1))?
            .with_column("sum", cast(col("c2") + col("c3"), DataType::Int64))?;

        let cached_df = df.clone().cache().await?;

        assert_eq!(
            "TableScan: ?table? projection=[c2, c3, sum]",
            format!("{:?}", cached_df.clone().into_optimized_plan()?)
        );

        let df_results = df.collect().await?;
        let cached_df_results = cached_df.collect().await?;
        assert_batches_sorted_eq!(
            [
                "+----+----+-----+",
                "| c2 | c3 | sum |",
                "+----+----+-----+",
                "| 2  | 1  | 3   |",
                "+----+----+-----+"
            ],
            &cached_df_results
        );

        assert_eq!(&df_results, &cached_df_results);

        Ok(())
    }

    #[tokio::test]
    async fn partition_aware_union() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c3"])?
            .with_column_renamed("c2.c1", "c2_c1")?;

        let left_rows = left.clone().collect().await?;
        let right_rows = right.clone().collect().await?;
        let join1 = left.clone().join(
            right.clone(),
            JoinType::Inner,
            &["c1"],
            &["c2_c1"],
            None,
        )?;
        let join2 = left.join(right, JoinType::Inner, &["c1"], &["c2_c1"], None)?;

        let union = join1.union(join2)?;

        let union_rows = union.clone().collect().await?;

        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(4016, union_rows.iter().map(|x| x.num_rows()).sum::<usize>());

        let physical_plan = union.create_physical_plan().await?;
        let default_partition_count = SessionConfig::new().target_partitions();

        // For partition aware union, the output partition count should not be changed.
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            default_partition_count
        );
        // For partition aware union, the output partition is the same with the union's inputs
        for child in physical_plan.children() {
            assert_eq!(
                physical_plan.output_partitioning(),
                child.output_partitioning()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn non_partition_aware_union() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c2"])?
            .with_column_renamed("c2.c1", "c2_c1")?
            .with_column_renamed("c2.c2", "c2_c2")?;

        let left_rows = left.clone().collect().await?;
        let right_rows = right.clone().collect().await?;
        let join1 = left.clone().join(
            right.clone(),
            JoinType::Inner,
            &["c1", "c2"],
            &["c2_c1", "c2_c2"],
            None,
        )?;

        // join key ordering is different
        let join2 = left.join(
            right,
            JoinType::Inner,
            &["c2", "c1"],
            &["c2_c2", "c2_c1"],
            None,
        )?;

        let union = join1.union(join2)?;

        let union_rows = union.clone().collect().await?;

        assert_eq!(100, left_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(100, right_rows.iter().map(|x| x.num_rows()).sum::<usize>());
        assert_eq!(916, union_rows.iter().map(|x| x.num_rows()).sum::<usize>());

        let physical_plan = union.create_physical_plan().await?;
        let default_partition_count = SessionConfig::new().target_partitions();

        // For non-partition aware union, the output partitioning count should be the combination of all output partitions count
        assert!(matches!(
            physical_plan.output_partitioning(),
            Partitioning::UnknownPartitioning(partition_count) if partition_count == default_partition_count * 2));
        Ok(())
    }

    #[tokio::test]
    async fn verify_join_output_partitioning() -> Result<()> {
        let left = test_table().await?.select_columns(&["c1", "c2"])?;
        let right = test_table_with_name("c2")
            .await?
            .select_columns(&["c1", "c2"])?
            .with_column_renamed("c2.c1", "c2_c1")?
            .with_column_renamed("c2.c2", "c2_c2")?;

        let all_join_types = vec![
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
        ];

        let default_partition_count = SessionConfig::new().target_partitions();

        for join_type in all_join_types {
            let join = left.clone().join(
                right.clone(),
                join_type,
                &["c1", "c2"],
                &["c2_c1", "c2_c2"],
                None,
            )?;
            let physical_plan = join.create_physical_plan().await?;
            let out_partitioning = physical_plan.output_partitioning();
            let join_schema = physical_plan.schema();

            match join_type {
                JoinType::Inner
                | JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti => {
                    let left_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                        Arc::new(Column::new_with_schema("c1", &join_schema)?),
                        Arc::new(Column::new_with_schema("c2", &join_schema)?),
                    ];
                    assert_eq!(
                        out_partitioning,
                        Partitioning::Hash(left_exprs, default_partition_count)
                    );
                }
                JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => {
                    let right_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                        Arc::new(Column::new_with_schema("c2_c1", &join_schema)?),
                        Arc::new(Column::new_with_schema("c2_c2", &join_schema)?),
                    ];
                    assert_eq!(
                        out_partitioning,
                        Partitioning::Hash(right_exprs, default_partition_count)
                    );
                }
                JoinType::Full => {
                    assert!(matches!(
                        out_partitioning,
                    Partitioning::UnknownPartitioning(partition_count) if partition_count == default_partition_count));
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn write_parquet_with_compression() -> Result<()> {
        let test_df = test_table().await?;

        let output_path = "file://local/test.parquet";
        let test_compressions = vec![
            parquet::basic::Compression::SNAPPY,
            parquet::basic::Compression::LZ4,
            parquet::basic::Compression::LZ4_RAW,
            parquet::basic::Compression::GZIP(GzipLevel::default()),
            parquet::basic::Compression::BROTLI(BrotliLevel::default()),
            parquet::basic::Compression::ZSTD(ZstdLevel::default()),
        ];
        for compression in test_compressions.into_iter() {
            let df = test_df.clone();
            let tmp_dir = TempDir::new()?;
            let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
            let local_url = Url::parse("file://local").unwrap();
            let ctx = &test_df.session_state;
            ctx.runtime_env().register_object_store(&local_url, local);
            df.write_parquet(
                output_path,
                DataFrameWriteOptions::new().with_single_file_output(true),
                Some(
                    WriterProperties::builder()
                        .set_compression(compression)
                        .build(),
                ),
            )
            .await?;

            // Check that file actually used the specified compression
            let file = std::fs::File::open(tmp_dir.into_path().join("test.parquet"))?;

            let reader =
                parquet::file::serialized_reader::SerializedFileReader::new(file)
                    .unwrap();

            let parquet_metadata = reader.metadata();

            let written_compression =
                parquet_metadata.row_group(0).column(0).compression();

            assert_eq!(written_compression, compression);
        }

        Ok(())
    }
}
