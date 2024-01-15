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

//! Data source traits

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::{not_impl_err, Constraints, DataFusionError, Statistics};
use datafusion_expr::{CreateExternalTable, LogicalPlan};
pub use datafusion_expr::{TableProviderFilterPushDown, TableType};

use crate::arrow::datatypes::SchemaRef;
use crate::datasource::listing_table_factory::ListingTableFactory;
use crate::datasource::stream::StreamTableFactory;
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::logical_expr::Expr;
use crate::physical_plan::ExecutionPlan;

/// Source table
#[async_trait]
pub trait TableProvider: Sync + Send {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
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

    /// Get the [`LogicalPlan`] of this table, if available
    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    #[deprecated(since = "20.0.0", note = "use supports_filters_pushdown instead")]
    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }

    /// Tests whether the table provider can make use of any or all filter expressions
    /// to optimise data retrieval.
    #[allow(deprecated)]
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| self.supports_filter_pushdown(f))
            .collect()
    }

    /// Get statistics for this table, if available
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
    /// See [`FileSinkExec`] for the common pattern of inserting a
    /// streams of `RecordBatch`es as files to an ObjectStore.
    ///
    /// [`FileSinkExec`]: crate::physical_plan::insert::FileSinkExec
    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Insert into not implemented for this table")
    }
}

/// A factory which creates [`TableProvider`]s at runtime given a URL.
///
/// For example, this can be used to create a table "on the fly"
/// from a directory of files only when that name is referenced.
#[async_trait]
pub trait TableProviderFactory: Sync + Send {
    /// Create a TableProvider with the given url
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>>;
}

/// The default [`TableProviderFactory`]
///
/// If [`CreateExternalTable`] is unbounded calls [`StreamTableFactory::create`],
/// otherwise calls [`ListingTableFactory::create`]
#[derive(Debug, Default)]
pub struct DefaultTableFactory {
    stream: StreamTableFactory,
    listing: ListingTableFactory,
}

impl DefaultTableFactory {
    /// Creates a new [`DefaultTableFactory`]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TableProviderFactory for DefaultTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let mut unbounded = cmd.unbounded;
        for (k, v) in &cmd.options {
            if k.eq_ignore_ascii_case("unbounded") && v.eq_ignore_ascii_case("true") {
                unbounded = true
            }
        }

        match unbounded {
            true => self.stream.create(state, cmd).await,
            false => self.listing.create(state, cmd).await,
        }
    }
}
