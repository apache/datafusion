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

use std::any::Any;
use std::borrow::Cow;
use std::fmt::Debug;
use std::sync::Arc;

use crate::session::Session;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::{Constraints, Statistics, not_impl_err};
use datafusion_common::{Result, internal_err};
use datafusion_expr::Expr;

use datafusion_expr::dml::InsertOp;
use datafusion_expr::{
    CreateExternalTable, LogicalPlan, TableProviderFilterPushDown, TableType,
};
use datafusion_physical_plan::ExecutionPlan;

/// A table which can be queried and modified.
///
/// Please see [`CatalogProvider`] for details of implementing a custom catalog.
///
/// [`TableProvider`] represents a source of data which can provide data as
/// Apache Arrow [`RecordBatch`]es. Implementations of this trait provide
/// important information for planning such as:
///
/// 1. [`Self::schema`]: The schema (columns and their types) of the table
/// 2. [`Self::supports_filters_pushdown`]: Should filters be pushed into this scan
/// 2. [`Self::scan`]: An [`ExecutionPlan`] that can read data
///
/// [`RecordBatch`]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
/// [`CatalogProvider`]: super::CatalogProvider
#[async_trait]
pub trait TableProvider: Debug + Sync + Send {
    /// Returns the table provider as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef;

    /// Get a reference to the constraints of the table.
    /// Returns:
    /// - `None` for tables that do not support constraints.
    /// - `Some(&Constraints)` for tables supporting constraints.
    /// Therefore, a `Some(&Constraints::empty())` return value indicates that
    /// this table supports constraints, but there are no constraints.
    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType;

    /// Get the create statement used to create this table, if available.
    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    /// Get the [`LogicalPlan`] of this table, if available.
    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    /// Get the default value for a column, if available.
    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    /// Create an [`ExecutionPlan`] for scanning the table with optionally
    /// specified `projection`, `filter` and `limit`, described below.
    ///
    /// The `ExecutionPlan` is responsible scanning the datasource's
    /// partitions in a streaming, parallelized fashion.
    ///
    /// # Projection
    ///
    /// If specified, only a subset of columns should be returned, in the order
    /// specified. The projection is a set of indexes of the fields in
    /// [`Self::schema`].
    ///
    /// DataFusion provides the projection to scan only the columns actually
    /// used in the query to improve performance, an optimization  called
    /// "Projection Pushdown". Some datasources, such as Parquet, can use this
    /// information to go significantly faster when only a subset of columns is
    /// required.
    ///
    /// # Filters
    ///
    /// A list of boolean filter [`Expr`]s to evaluate *during* the scan, in the
    /// manner specified by [`Self::supports_filters_pushdown`]. Only rows for
    /// which *all* of the `Expr`s evaluate to `true` must be returned (aka the
    /// expressions are `AND`ed together).
    ///
    /// To enable filter pushdown you must override
    /// [`Self::supports_filters_pushdown`] as the default implementation does
    /// not and `filters` will be empty.
    ///
    /// DataFusion pushes filtering into the scans whenever possible
    /// ("Filter Pushdown"), and depending on the format and the
    /// implementation of the format, evaluating the predicate during the scan
    /// can increase performance significantly.
    ///
    /// ## Note: Some columns may appear *only* in Filters
    ///
    /// In certain cases, a query may only use a certain column in a Filter that
    /// has been completely pushed down to the scan. In this case, the
    /// projection will not contain all the columns found in the filter
    /// expressions.
    ///
    /// For example, given the query `SELECT t.a FROM t WHERE t.b > 5`,
    ///
    /// ```text
    /// ┌────────────────────┐
    /// │  Projection(t.a)   │
    /// └────────────────────┘
    ///            ▲
    ///            │
    ///            │
    /// ┌────────────────────┐     Filter     ┌────────────────────┐   Projection    ┌────────────────────┐
    /// │  Filter(t.b > 5)   │────Pushdown──▶ │  Projection(t.a)   │ ───Pushdown───▶ │  Projection(t.a)   │
    /// └────────────────────┘                └────────────────────┘                 └────────────────────┘
    ///            ▲                                     ▲                                      ▲
    ///            │                                     │                                      │
    ///            │                                     │                           ┌────────────────────┐
    /// ┌────────────────────┐                ┌────────────────────┐                 │        Scan        │
    /// │        Scan        │                │        Scan        │                 │  filter=(t.b > 5)  │
    /// └────────────────────┘                │  filter=(t.b > 5)  │                 │  projection=(t.a)  │
    ///                                       └────────────────────┘                 └────────────────────┘
    ///
    /// Initial Plan                  If `TableProviderFilterPushDown`           Projection pushdown notes that
    ///                               returns true, filter pushdown              the scan only needs t.a
    ///                               pushes the filter into the scan
    ///                                                                          BUT internally evaluating the
    ///                                                                          predicate still requires t.b
    /// ```
    ///
    /// # Limit
    ///
    /// If `limit` is specified,  must only produce *at least* this many rows,
    /// (though it may return more).  Like Projection Pushdown and Filter
    /// Pushdown, DataFusion pushes `LIMIT`s  as far down in the plan as
    /// possible, called "Limit Pushdown" as some sources can use this
    /// information to improve their performance. Note that if there are any
    /// Inexact filters pushed down, the LIMIT cannot be pushed down. This is
    /// because inexact filters do not guarantee that every filtered row is
    /// removed, so applying the limit could lead to too few rows being available
    /// to return as a final result.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Create an [`ExecutionPlan`] for scanning the table using structured arguments.
    ///
    /// This method uses [`ScanArgs`] to pass scan parameters in a structured way
    /// and returns a [`ScanResult`] containing the execution plan.
    ///
    /// Table providers can override this method to take advantage of additional
    /// parameters like the upcoming `preferred_ordering` that may not be available through
    /// other scan methods.
    ///
    /// # Arguments
    /// * `state` - The session state containing configuration and context
    /// * `args` - Structured scan arguments including projection, filters, limit, and ordering preferences
    ///
    /// # Returns
    /// A [`ScanResult`] containing the [`ExecutionPlan`] for scanning the table
    ///
    /// See [`Self::scan`] for detailed documentation about projection, filters, and limits.
    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        let filters = args.filters().unwrap_or(&[]);
        let projection = args.projection().map(|p| p.to_vec());
        let limit = args.limit();
        let plan = self
            .scan(state, projection.as_ref(), filters, limit)
            .await?;
        Ok(plan.into())
    }

    /// Specify if DataFusion should provide filter expressions to the
    /// TableProvider to apply *during* the scan.
    ///
    /// Some TableProviders can evaluate filters more efficiently than the
    /// `Filter` operator in DataFusion, for example by using an index.
    ///
    /// # Parameters and Return Value
    ///
    /// The return `Vec` must have one element for each element of the `filters`
    /// argument. The value of each element indicates if the TableProvider can
    /// apply the corresponding filter during the scan. The position in the return
    /// value corresponds to the expression in the `filters` parameter.
    ///
    /// If the length of the resulting `Vec` does not match the `filters` input
    /// an error will be thrown.
    ///
    /// Each element in the resulting `Vec` is one of the following:
    /// * [`Exact`] or [`Inexact`]: The TableProvider can apply the filter
    /// during scan
    /// * [`Unsupported`]: The TableProvider cannot apply the filter during scan
    ///
    /// By default, this function returns [`Unsupported`] for all filters,
    /// meaning no filters will be provided to [`Self::scan`].
    ///
    /// [`Unsupported`]: TableProviderFilterPushDown::Unsupported
    /// [`Exact`]: TableProviderFilterPushDown::Exact
    /// [`Inexact`]: TableProviderFilterPushDown::Inexact
    /// # Example
    ///
    /// ```rust
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::SchemaRef;
    /// # use async_trait::async_trait;
    /// # use datafusion_catalog::{TableProvider, Session};
    /// # use datafusion_common::Result;
    /// # use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
    /// # use datafusion_physical_plan::ExecutionPlan;
    /// // Define a struct that implements the TableProvider trait
    /// #[derive(Debug)]
    /// struct TestDataSource {}
    ///
    /// #[async_trait]
    /// impl TableProvider for TestDataSource {
    /// # fn as_any(&self) -> &dyn Any { todo!() }
    /// # fn schema(&self) -> SchemaRef { todo!() }
    /// # fn table_type(&self) -> TableType { todo!() }
    /// # async fn scan(&self, s: &dyn Session, p: Option<&Vec<usize>>, f: &[Expr], l: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
    ///         todo!()
    /// # }
    ///     // Override the supports_filters_pushdown to evaluate which expressions
    ///     // to accept as pushdown predicates.
    ///     fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
    ///         // Process each filter
    ///         let support: Vec<_> = filters.iter().map(|expr| {
    ///           match expr {
    ///             // This example only supports a between expr with a single column named "c1".
    ///             Expr::Between(between_expr) => {
    ///                 between_expr.expr
    ///                 .try_as_col()
    ///                 .map(|column| {
    ///                     if column.name == "c1" {
    ///                         TableProviderFilterPushDown::Exact
    ///                     } else {
    ///                         TableProviderFilterPushDown::Unsupported
    ///                     }
    ///                 })
    ///                 // If there is no column in the expr set the filter to unsupported.
    ///                 .unwrap_or(TableProviderFilterPushDown::Unsupported)
    ///             }
    ///             _ => {
    ///                 // For all other cases return Unsupported.
    ///                 TableProviderFilterPushDown::Unsupported
    ///             }
    ///         }
    ///     }).collect();
    ///     Ok(support)
    ///     }
    /// }
    /// ```
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    /// Get statistics for this table, if available
    /// Although not presently used in mainline DataFusion, this allows implementation specific
    /// behavior for downstream repositories, in conjunction with specialized optimizer rules to
    /// perform operations such as re-ordering of joins.
    fn statistics(&self) -> Option<Statistics> {
        None
    }

    /// Return an [`ExecutionPlan`] to insert data into this table, if
    /// supported.
    ///
    /// The returned plan should return a single row in a UInt64
    /// column called "count" such as the following
    ///
    /// ```text
    /// +-------+,
    /// | count |,
    /// +-------+,
    /// | 6     |,
    /// +-------+,
    /// ```
    ///
    /// # See Also
    ///
    /// See [`DataSinkExec`] for the common pattern of inserting a
    /// streams of `RecordBatch`es as files to an ObjectStore.
    ///
    /// [`DataSinkExec`]: datafusion_datasource::sink::DataSinkExec
    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Insert into not implemented for this table")
    }

    /// Delete rows matching the filter predicates.
    ///
    /// Returns an [`ExecutionPlan`] producing a single row with `count` (UInt64).
    /// Empty `filters` deletes all rows.
    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("DELETE not supported for {} table", self.table_type())
    }

    /// Update rows matching the filter predicates.
    ///
    /// Returns an [`ExecutionPlan`] producing a single row with `count` (UInt64).
    /// Empty `filters` updates all rows.
    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("UPDATE not supported for {} table", self.table_type())
    }

    /// Remove all rows from the table.
    ///
    /// Should return an [ExecutionPlan] producing a single row with count (UInt64),
    /// representing the number of rows removed.
    async fn truncate(&self, _state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("TRUNCATE not supported for {} table", self.table_type())
    }
}

/// Arguments for scanning a table with [`TableProvider::scan_with_args`].
#[derive(Debug, Clone, Default)]
pub struct ScanArgs<'a> {
    filters: Option<&'a [Expr]>,
    projection: Option<&'a [usize]>,
    limit: Option<usize>,
}

impl<'a> ScanArgs<'a> {
    /// Set the column projection for the scan.
    ///
    /// The projection is a list of column indices from [`TableProvider::schema`]
    /// that should be included in the scan results. If `None`, all columns are included.
    ///
    /// # Arguments
    /// * `projection` - Optional slice of column indices to project
    pub fn with_projection(mut self, projection: Option<&'a [usize]>) -> Self {
        self.projection = projection;
        self
    }

    /// Get the column projection for the scan.
    ///
    /// Returns a reference to the projection column indices, or `None` if
    /// no projection was specified (meaning all columns should be included).
    pub fn projection(&self) -> Option<&'a [usize]> {
        self.projection
    }

    /// Set the filter expressions for the scan.
    ///
    /// Filters are boolean expressions that should be evaluated during the scan
    /// to reduce the number of rows returned. All expressions are combined with AND logic.
    /// Whether filters are actually pushed down depends on [`TableProvider::supports_filters_pushdown`].
    ///
    /// # Arguments
    /// * `filters` - Optional slice of filter expressions
    pub fn with_filters(mut self, filters: Option<&'a [Expr]>) -> Self {
        self.filters = filters;
        self
    }

    /// Get the filter expressions for the scan.
    ///
    /// Returns a reference to the filter expressions, or `None` if no filters were specified.
    pub fn filters(&self) -> Option<&'a [Expr]> {
        self.filters
    }

    /// Set the maximum number of rows to return from the scan.
    ///
    /// If specified, the scan should return at most this many rows. This is typically
    /// used to optimize queries with `LIMIT` clauses.
    ///
    /// # Arguments
    /// * `limit` - Optional maximum number of rows to return
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Get the maximum number of rows to return from the scan.
    ///
    /// Returns the row limit, or `None` if no limit was specified.
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}

/// Result of a table scan operation from [`TableProvider::scan_with_args`].
#[derive(Debug, Clone)]
pub struct ScanResult {
    /// The ExecutionPlan to run.
    plan: Arc<dyn ExecutionPlan>,
}

impl ScanResult {
    /// Create a new `ScanResult` with the given execution plan.
    ///
    /// # Arguments
    /// * `plan` - The execution plan that will perform the table scan
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }

    /// Get a reference to the execution plan for this scan result.
    ///
    /// Returns a reference to the [`ExecutionPlan`] that will perform
    /// the actual table scanning and data retrieval.
    pub fn plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }

    /// Consume this ScanResult and return the execution plan.
    ///
    /// Returns the owned [`ExecutionPlan`] that will perform
    /// the actual table scanning and data retrieval.
    pub fn into_inner(self) -> Arc<dyn ExecutionPlan> {
        self.plan
    }
}

impl From<Arc<dyn ExecutionPlan>> for ScanResult {
    fn from(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self::new(plan)
    }
}

/// A factory which creates [`TableProvider`]s at runtime given a URL.
///
/// For example, this can be used to create a table "on the fly"
/// from a directory of files only when that name is referenced.
#[async_trait]
pub trait TableProviderFactory: Debug + Sync + Send {
    /// Create a TableProvider with the given url
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>>;
}

/// Describes arguments provided to the table function call.
pub struct TableFunctionArgs<'a> {
    /// Call arguments.
    pub args: &'a [Expr],
    /// Session within which the function is called.
    pub session: &'a dyn Session,
}

/// A trait for table function implementations
pub trait TableFunctionImpl: Debug + Sync + Send {
    /// Create a table provider
    #[deprecated(
        since = "53.0.0",
        note = "Implement `TableFunctionImpl::call_with_args` instead"
    )]
    fn call(&self, _args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        internal_err!("unimplemented")
    }

    /// Create a table provider
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        #[expect(deprecated)]
        self.call(args.args)
    }
}

/// A table that uses a function to generate data
#[derive(Clone, Debug)]
pub struct TableFunction {
    /// Name of the table function
    name: String,
    /// Function implementation
    fun: Arc<dyn TableFunctionImpl>,
}

impl TableFunction {
    /// Create a new table function
    pub fn new(name: String, fun: Arc<dyn TableFunctionImpl>) -> Self {
        Self { name, fun }
    }

    /// Get the name of the table function
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the implementation of the table function
    pub fn function(&self) -> &Arc<dyn TableFunctionImpl> {
        &self.fun
    }

    /// Get the function implementation and generate a table
    #[deprecated(
        since = "53.0.0",
        note = "Use `TableFunction::create_table_provider_with_args` instead"
    )]
    pub fn create_table_provider(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        #[expect(deprecated)]
        self.fun.call(args)
    }

    /// Get the function implementation and generate a table
    pub fn create_table_provider_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> Result<Arc<dyn TableProvider>> {
        self.fun.call_with_args(args)
    }
}
