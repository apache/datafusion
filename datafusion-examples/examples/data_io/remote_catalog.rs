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

//! See `main.rs` for how to run it.
//!
/// This example shows how to implement the DataFusion [`CatalogProvider`] API
/// for catalogs that are remote (require network access) and/or offer only
/// asynchronous APIs such as [Polaris], [Unity], and [Hive].
///
/// Integrating with this catalogs is a bit more complex than with local
/// catalogs because calls like `ctx.sql("SELECT * FROM db.schm.tbl")` may need
/// to perform remote network requests, but many Catalog APIs are synchronous.
/// See the documentation on [`CatalogProvider`] for more details.
///
/// [`CatalogProvider`]: datafusion_catalog::CatalogProvider
///
/// [Polaris]: https://github.com/apache/polaris
/// [Unity]: https://github.com/unitycatalog/unitycatalog
/// [Hive]: https://hive.apache.org/
use arrow::array::record_batch;
use arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::catalog::{AsyncSchemaProvider, Session};
use datafusion::common::Result;
use datafusion::common::{assert_batches_eq, internal_datafusion_err, plan_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{DataFrame, SessionContext};
use futures::TryStreamExt;
use std::any::Any;
use std::sync::Arc;

/// Interfacing with a remote catalog (e.g. over a network)
pub async fn remote_catalog() -> Result<()> {
    // As always, we create a session context to interact with DataFusion
    let ctx = SessionContext::new();

    // Make a connection to the remote catalog, asynchronously, and configure it
    let remote_catalog_interface = Arc::new(RemoteCatalogInterface::connect().await?);

    // Create an adapter to provide the AsyncSchemaProvider interface to DataFusion
    // based on our remote catalog interface
    let remote_catalog_adapter = RemoteCatalogDatafusionAdapter(remote_catalog_interface);

    // Here is a query that selects data from a table in the remote catalog.
    let sql = "SELECT * from remote_schema.remote_table";

    // The `SessionContext::sql` interface is async, but it does not
    // support asynchronous access to catalogs, so we cannot register our schema provider
    // directly and the following query fails to find our table.
    let results = ctx.sql(sql).await;
    assert_eq!(
        results.unwrap_err().to_string(),
        "Error during planning: table 'datafusion.remote_schema.remote_table' not found"
    );

    // Instead, to use a remote catalog, we must use lower level APIs on
    // SessionState (what `SessionContext::sql` does internally).
    let state = ctx.state();

    // First, parse the SQL (but don't plan it / resolve any table references)
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(sql, &dialect)?;

    // Find all `TableReferences` in the parsed queries. These correspond to the
    // tables referred to by the query (in this case
    // `remote_schema.remote_table`)
    let references = state.resolve_table_references(&statement)?;

    // Now we can asynchronously resolve the table references to get a cached catalog
    // that we can use for our query
    let resolved_catalog = remote_catalog_adapter
        .resolve(&references, state.config(), "datafusion", "remote_schema")
        .await?;

    // This resolved catalog only makes sense for this query and so we create a clone
    // of the session context with the resolved catalog
    let query_ctx = ctx.clone();

    query_ctx
        .catalog("datafusion")
        .ok_or_else(|| internal_datafusion_err!("default catalog was not installed"))?
        .register_schema("remote_schema", resolved_catalog)?;

    // We can now continue planning the query with this new query-specific context that
    // contains our cached catalog
    let query_state = query_ctx.state();

    let plan = query_state.statement_to_plan(statement).await?;
    let results = DataFrame::new(state, plan).collect().await?;
    assert_batches_eq!(
        [
            "+----+-------+",
            "| id | name  |",
            "+----+-------+",
            "| 1  | alpha |",
            "| 2  | beta  |",
            "| 3  | gamma |",
            "+----+-------+",
        ],
        &results
    );

    Ok(())
}

/// This is an example of an API that interacts with a remote catalog.
///
/// Specifically, its APIs are all `async` and thus can not be used by
/// [`SchemaProvider`] or [`TableProvider`] directly.
#[derive(Debug)]
struct RemoteCatalogInterface {}

impl RemoteCatalogInterface {
    /// Establish a connection to the remote catalog
    pub async fn connect() -> Result<Self> {
        // In a real implementation this method might connect to a remote
        // catalog, validate credentials, cache basic information, etc
        Ok(Self {})
    }

    /// Fetches information for a specific table
    pub async fn table_info(&self, name: &str) -> Result<Option<SchemaRef>> {
        if name != "remote_table" {
            return Ok(None);
        }

        // In this example, we'll model a remote table with columns "id" and
        // "name"
        //
        // A real remote catalog would  make a network call to fetch this
        // information from a remote source.
        let schema = Schema::new(Fields::from(vec![
            Field::new("id", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));
        Ok(Some(Arc::new(schema)))
    }

    /// Fetches data for a table from a remote data source
    pub async fn read_data(&self, name: &str) -> Result<SendableRecordBatchStream> {
        if name != "remote_table" {
            return plan_err!("Remote table not found: {}", name);
        }

        // In a real remote catalog this call would likely perform network IO to
        // open and begin reading from a remote datasource, prefetching
        // information, etc.
        //
        // In this example we are just demonstrating how the API works so simply
        // return back some static data as a stream.
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("name", Utf8, ["alpha", "beta", "gamma"])
        )
        .unwrap();
        let schema = batch.schema();

        let stream = futures::stream::iter([Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Implements an async version of the DataFusion SchemaProvider API for tables
/// stored in a remote catalog.
struct RemoteCatalogDatafusionAdapter(Arc<RemoteCatalogInterface>);

#[async_trait]
impl AsyncSchemaProvider for RemoteCatalogDatafusionAdapter {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Fetch information about the table from the remote catalog
        //
        // Note that a real remote catalog interface could return more
        // information, but at the minimum, DataFusion requires the
        // table's schema for planing.
        Ok(self.0.table_info(name).await?.map(|schema| {
            Arc::new(RemoteTable::new(Arc::clone(&self.0), name, schema))
                as Arc<dyn TableProvider>
        }))
    }
}

/// Represents the information about a table retrieved from the remote catalog
#[derive(Debug)]
struct RemoteTable {
    /// connection to the remote catalog
    remote_catalog_interface: Arc<RemoteCatalogInterface>,
    name: String,
    schema: SchemaRef,
}

impl RemoteTable {
    pub fn new(
        remote_catalog_interface: Arc<RemoteCatalogInterface>,
        name: impl Into<String>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            remote_catalog_interface,
            name: name.into(),
            schema,
        }
    }
}

/// Implement the DataFusion Catalog API for [`RemoteTable`]
#[async_trait]
impl TableProvider for RemoteTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Note that `scan` is called once the plan begin execution, and thus is
        // async. When interacting with remote data sources, this is the place
        // to begin establishing the remote connections and interacting with the
        // remote storage system.
        //
        // As this example is just modeling the catalog API interface, we buffer
        // the results locally in memory for simplicity.
        let batches = self
            .remote_catalog_interface
            .read_data(&self.name)
            .await?
            .try_collect()
            .await?;
        let exec = MemorySourceConfig::try_new_exec(
            &[batches],
            self.schema.clone(),
            projection.cloned(),
        )?;
        Ok(exec)
    }
}
