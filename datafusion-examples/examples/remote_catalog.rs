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
use arrow_schema::{Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_catalog::Session;
use datafusion_common::{
    assert_batches_eq, internal_datafusion_err, plan_err, HashMap, TableReference,
};
use datafusion_expr::{Expr, TableType};
use futures::TryStreamExt;
use std::any::Any;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() -> Result<()> {
    // As always, we create a session context to interact with DataFusion
    let ctx = SessionContext::new();

    // Make a connection to the remote catalog, asynchronously, and configure it
    let remote_catalog_interface = RemoteCatalogInterface::connect().await?;

    // Register a SchemaProvider for tables in a schema named "remote_schema".
    //
    // This will let DataFusion query tables such as
    // `datafusion.remote_schema.remote_table`
    let remote_schema: Arc<dyn SchemaProvider> =
        Arc::new(RemoteSchema::new(Arc::new(remote_catalog_interface)));
    ctx.catalog("datafusion")
        .ok_or_else(|| internal_datafusion_err!("default catalog was not installed"))?
        .register_schema("remote_schema", Arc::clone(&remote_schema))?;

    // Here is a query that selects data from a table in the remote catalog.
    let sql = "SELECT * from remote_schema.remote_table";

    // The `SessionContext::sql` interface is async, but it does not
    // support asynchronous access to catalogs, so the following query errors.
    let results = ctx.sql(sql).await;
    assert_eq!(
        results.unwrap_err().to_string(),
        "Error during planning: table 'datafusion.remote_schema.remote_table' not found"
    );

    // Instead, to use a remote catalog, we must use lower level APIs on
    // SessionState (what `SessionContext::sql` does internally).
    let state = ctx.state();

    // First, parse the SQL (but don't plan it / resolve any table references)
    let dialect = state.config().options().sql_parser.dialect.as_str();
    let statement = state.sql_to_statement(sql, dialect)?;

    // Find all `TableReferences` in the parsed queries. These correspond to the
    // tables referred to by the query (in this case
    // `remote_schema.remote_table`)
    let references = state.resolve_table_references(&statement)?;

    // Call `load_tables` to load information from the remote catalog for each
    // of the referenced tables. Best practice is to fetch the the information
    // for all tables required by the query once (rather than one per table) to
    // minimize network overhead
    let table_names = references.iter().filter_map(|r| {
        if refers_to_schema("datafusion", "remote_schema", r) {
            Some(r.table())
        } else {
            None
        }
    });
    remote_schema
        .as_any()
        .downcast_ref::<RemoteSchema>()
        .expect("correct types")
        .load_tables(table_names)
        .await?;

    // Now continue planing the query after having fetched the remote table and
    // it can run as normal
    let plan = state.statement_to_plan(statement).await?;
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
    pub async fn table_info(&self, name: &str) -> Result<SchemaRef> {
        if name != "remote_table" {
            return plan_err!("Remote table not found: {}", name);
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
        Ok(Arc::new(schema))
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

/// Implements the DataFusion Catalog API interface for tables
/// stored in a remote catalog.
#[derive(Debug)]
struct RemoteSchema {
    /// Connection with the remote catalog
    remote_catalog_interface: Arc<RemoteCatalogInterface>,
    /// Local cache of tables that have been preloaded from the remote
    /// catalog
    tables: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl RemoteSchema {
    /// Create a new RemoteSchema
    pub fn new(remote_catalog_interface: Arc<RemoteCatalogInterface>) -> Self {
        Self {
            remote_catalog_interface,
            tables: Mutex::new(HashMap::new()),
        }
    }

    /// Load information for the specified tables from the remote source into
    /// the local cached copy.
    pub async fn load_tables(
        &self,
        references: impl IntoIterator<Item = &str>,
    ) -> Result<()> {
        for table_name in references {
            if !self.table_exist(table_name) {
                // Fetch information about the table from the remote catalog
                //
                // Note that a real remote catalog interface could return more
                // information, but at the minimum, DataFusion requires the
                // table's schema for planing.
                let schema = self.remote_catalog_interface.table_info(table_name).await?;
                let remote_table = RemoteTable::new(
                    Arc::clone(&self.remote_catalog_interface),
                    table_name,
                    schema,
                );

                // Add the table to our local cached list
                self.tables
                    .lock()
                    .expect("mutex invalid")
                    .insert(table_name.to_string(), Arc::new(remote_table));
            };
        }
        Ok(())
    }
}

/// Implement the DataFusion Catalog API for [`RemoteSchema`]
#[async_trait]
impl SchemaProvider for RemoteSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Note this API is not async so we can't directly call the RemoteCatalogInterface
        // instead we use the cached list of loaded tables
        self.tables
            .lock()
            .expect("mutex valid")
            .keys()
            .cloned()
            .collect()
    }

    // While this API is actually `async` and thus could consult a remote
    // catalog directly it is more efficient to use a local cached copy instead,
    // which is what we model in this example
    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        // Look for any pre-loaded tables
        let table = self
            .tables
            .lock()
            .expect("mutex valid")
            .get(name)
            .map(Arc::clone);
        Ok(table)
    }

    fn table_exist(&self, name: &str) -> bool {
        // Look for any pre-loaded tables, note this function is also `async`
        self.tables.lock().expect("mutex valid").contains_key(name)
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
        Ok(Arc::new(MemoryExec::try_new(
            &[batches],
            self.schema.clone(),
            projection.cloned(),
        )?))
    }
}

/// Return true if this `table_reference` might be for a table in the specified
/// catalog and schema.
fn refers_to_schema(
    catalog_name: &str,
    schema_name: &str,
    table_reference: &TableReference,
) -> bool {
    // Check the references are in the correct catalog and schema
    // references like foo.bar.baz
    if let Some(catalog) = table_reference.catalog() {
        if catalog != catalog_name {
            return false;
        }
    }
    // references like bar.baz
    if let Some(schema) = table_reference.schema() {
        if schema != schema_name {
            return false;
        }
    }

    true
}
