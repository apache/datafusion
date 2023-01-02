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

//! SessionContext contains methods for registering data sources and executing queries
use crate::{
    catalog::catalog::{CatalogList, MemoryCatalogList},
    config::{
        OPT_COLLECT_STATISTICS, OPT_CREATE_DEFAULT_CATALOG_AND_SCHEMA,
        OPT_INFORMATION_SCHEMA, OPT_PARQUET_ENABLE_PRUNING, OPT_REPARTITION_AGGREGATIONS,
        OPT_REPARTITION_JOINS, OPT_REPARTITION_WINDOWS, OPT_TARGET_PARTITIONS,
    },
    datasource::listing::{ListingOptions, ListingTable},
    datasource::{MemTable, ViewTable},
    logical_expr::{PlanType, ToStringifiedPlan},
    optimizer::optimizer::Optimizer,
    physical_optimizer::{
        aggregate_statistics::AggregateStatistics, join_selection::JoinSelection,
        optimizer::PhysicalOptimizerRule,
    },
};
pub use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::var_provider::is_system_variables;
use parking_lot::RwLock;
use std::str::FromStr;
use std::sync::Arc;
use std::{
    any::{Any, TypeId},
    hash::{BuildHasherDefault, Hasher},
    string::String,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::catalog::{
    catalog::{CatalogProvider, MemoryCatalogProvider},
    schema::{MemorySchemaProvider, SchemaProvider},
};
use crate::dataframe::DataFrame;
use crate::datasource::{
    listing::{ListingTableConfig, ListingTableUrl},
    provider_as_source, TableProvider,
};
use crate::error::{DataFusionError, Result};
use crate::logical_expr::{
    CreateCatalog, CreateCatalogSchema, CreateExternalTable, CreateMemoryTable,
    CreateView, DropTable, DropView, Explain, LogicalPlan, LogicalPlanBuilder,
    SetVariable, TableSource, TableType, UNNAMED_TABLE,
};
use crate::optimizer::{OptimizerContext, OptimizerRule};
use datafusion_sql::{ResolvedTableReference, TableReference};

use crate::physical_optimizer::coalesce_batches::CoalesceBatches;
use crate::physical_optimizer::repartition::Repartition;

use crate::config::{
    ConfigOptions, OPT_BATCH_SIZE, OPT_COALESCE_BATCHES,
    OPT_ENABLE_ROUND_ROBIN_REPARTITION, OPT_FILTER_NULL_JOIN_KEYS,
    OPT_OPTIMIZER_MAX_PASSES, OPT_OPTIMIZER_SKIP_FAILED_RULES,
};
use crate::execution::{runtime_env::RuntimeEnv, FunctionRegistry};
use crate::physical_optimizer::enforcement::EnforceDistribution;
use crate::physical_plan::file_format::{plan_to_csv, plan_to_json, plan_to_parquet};
use crate::physical_plan::planner::DefaultPhysicalPlanner;
use crate::physical_plan::udaf::AggregateUDF;
use crate::physical_plan::udf::ScalarUDF;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::PhysicalPlanner;
use crate::variable::{VarProvider, VarType};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion_common::ScalarValue;
use datafusion_sql::{
    parser::DFParser,
    planner::{ContextProvider, SqlToRel},
};
use parquet::file::properties::WriterProperties;
use url::Url;

use crate::catalog::information_schema::{InformationSchemaProvider, INFORMATION_SCHEMA};
use crate::catalog::listing_schema::ListingSchemaProvider;
use crate::datasource::object_store::ObjectStoreUrl;
use crate::execution::memory_pool::MemoryPool;
use crate::physical_optimizer::global_sort_selection::GlobalSortSelection;
use crate::physical_optimizer::pipeline_checker::PipelineChecker;
use crate::physical_optimizer::pipeline_fixer::PipelineFixer;
use crate::physical_optimizer::sort_enforcement::EnforceSorting;
use uuid::Uuid;

use super::options::{
    AvroReadOptions, CsvReadOptions, NdJsonReadOptions, ParquetReadOptions,
};

/// The default catalog name - this impacts what SQL queries use if not specified
const DEFAULT_CATALOG: &str = "datafusion";
/// The default schema name - this impacts what SQL queries use if not specified
const DEFAULT_SCHEMA: &str = "public";

/// SessionContext is the main interface for executing queries with DataFusion. It stands for
/// the connection between user and DataFusion/Ballista cluster.
/// The context provides the following functionality
///
/// * Create DataFrame from a CSV or Parquet data source.
/// * Register a CSV or Parquet data source as a table that can be referenced from a SQL query.
/// * Register a custom data source that can be referenced from a SQL query.
/// * Execution a SQL query
///
/// The following example demonstrates how to use the context to execute a query against a CSV
/// data source using the DataFrame API:
///
/// ```
/// use datafusion::prelude::*;
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
///
/// The following example demonstrates how to execute the same query using SQL:
///
/// ```
/// use datafusion::prelude::*;
///
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let mut ctx = SessionContext::new();
/// ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).await?;
/// let results = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SessionContext {
    /// Uuid for the session
    session_id: String,
    /// Session start time
    session_start_time: DateTime<Utc>,
    /// Shared session state for the session
    state: Arc<RwLock<SessionState>>,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionContext {
    /// Creates a new execution context using a default session configuration.
    pub fn new() -> Self {
        Self::with_config(SessionConfig::new())
    }

    /// Finds any ListSchemaProviders and instructs them to reload tables from "disk"
    pub async fn refresh_catalogs(&self) -> Result<()> {
        let cat_names = self.catalog_names().clone();
        for cat_name in cat_names.iter() {
            let cat = self.catalog(cat_name.as_str()).ok_or_else(|| {
                DataFusionError::Internal("Catalog not found!".to_string())
            })?;
            for schema_name in cat.schema_names() {
                let schema = cat.schema(schema_name.as_str()).ok_or_else(|| {
                    DataFusionError::Internal("Schema not found!".to_string())
                })?;
                let lister = schema.as_any().downcast_ref::<ListingSchemaProvider>();
                if let Some(lister) = lister {
                    lister.refresh(&self.state()).await?;
                }
            }
        }
        Ok(())
    }

    /// Creates a new session context using the provided session configuration.
    pub fn with_config(config: SessionConfig) -> Self {
        let runtime = Arc::new(RuntimeEnv::default());
        Self::with_config_rt(config, runtime)
    }

    /// Creates a new session context using the provided configuration and RuntimeEnv.
    pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let state = SessionState::with_config_rt(config, runtime);
        Self::with_state(state)
    }

    /// Creates a new session context using the provided session state.
    pub fn with_state(state: SessionState) -> Self {
        Self {
            session_id: state.session_id.clone(),
            session_start_time: Utc::now(),
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Returns the time this session was created
    pub fn session_start_time(&self) -> DateTime<Utc> {
        self.session_start_time
    }

    /// Registers the [`RecordBatch`] as the specified table name
    pub fn register_batch(
        &self,
        table_name: &str,
        batch: RecordBatch,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        self.register_table(TableReference::Bare { table: table_name }, Arc::new(table))
    }

    /// Return the [RuntimeEnv] used to run queries with this [SessionContext]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.state.read().runtime_env.clone()
    }

    /// Return the session_id of this Session
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return a copied version of config for this Session
    pub fn copied_config(&self) -> SessionConfig {
        self.state.read().config.clone()
    }

    /// Creates a [`DataFrame`] that will execute a SQL query.
    ///
    /// This method is `async` because queries of type `CREATE EXTERNAL TABLE`
    /// might require the schema to be inferred.
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let mut statements = DFParser::parse_sql(sql)?;
        if statements.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        // create a query planner
        let plan = {
            // TODO: Move catalog off SessionState onto SessionContext
            let state = self.state.read();
            let query_planner = SqlToRel::new(&*state);
            query_planner.statement_to_plan(statements.pop_front().unwrap())?
        };

        match plan {
            LogicalPlan::CreateExternalTable(cmd) => {
                self.create_external_table(&cmd).await
            }

            LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                name,
                input,
                if_not_exists,
                or_replace,
            }) => {
                let input = Arc::try_unwrap(input).unwrap_or_else(|e| e.as_ref().clone());
                let table = self.table(&name);

                match (if_not_exists, or_replace, table) {
                    (true, false, Ok(_)) => self.return_empty_dataframe(),
                    (false, true, Ok(_)) => {
                        self.deregister_table(&name)?;
                        let schema = Arc::new(input.schema().as_ref().into());
                        let physical = DataFrame::new(self.state(), input);

                        let batches: Vec<_> = physical.collect_partitioned().await?;
                        let table = Arc::new(MemTable::try_new(schema, batches)?);

                        self.register_table(&name, table)?;
                        self.return_empty_dataframe()
                    }
                    (true, true, Ok(_)) => Err(DataFusionError::Execution(
                        "'IF NOT EXISTS' cannot coexist with 'REPLACE'".to_string(),
                    )),
                    (_, _, Err(_)) => {
                        let schema = Arc::new(input.schema().as_ref().into());
                        let physical = DataFrame::new(self.state(), input);

                        let batches: Vec<_> = physical.collect_partitioned().await?;
                        let table = Arc::new(MemTable::try_new(schema, batches)?);

                        self.register_table(&name, table)?;
                        self.return_empty_dataframe()
                    }
                    (false, false, Ok(_)) => Err(DataFusionError::Execution(format!(
                        "Table '{name}' already exists"
                    ))),
                }
            }

            LogicalPlan::CreateView(CreateView {
                name,
                input,
                or_replace,
                definition,
            }) => {
                let view = self.table(&name);

                match (or_replace, view) {
                    (true, Ok(_)) => {
                        self.deregister_table(&name)?;
                        let table =
                            Arc::new(ViewTable::try_new((*input).clone(), definition)?);

                        self.register_table(&name, table)?;
                        self.return_empty_dataframe()
                    }
                    (_, Err(_)) => {
                        let table =
                            Arc::new(ViewTable::try_new((*input).clone(), definition)?);

                        self.register_table(&name, table)?;
                        self.return_empty_dataframe()
                    }
                    (false, Ok(_)) => Err(DataFusionError::Execution(format!(
                        "Table '{name}' already exists"
                    ))),
                }
            }

            LogicalPlan::DropTable(DropTable {
                name, if_exists, ..
            }) => {
                let result = self.find_and_deregister(&name, TableType::Base);
                match (result, if_exists) {
                    (Ok(true), _) => self.return_empty_dataframe(),
                    (_, true) => self.return_empty_dataframe(),
                    (_, _) => Err(DataFusionError::Execution(format!(
                        "Table '{name}' doesn't exist."
                    ))),
                }
            }

            LogicalPlan::DropView(DropView {
                name, if_exists, ..
            }) => {
                let result = self.find_and_deregister(&name, TableType::View);
                match (result, if_exists) {
                    (Ok(true), _) => self.return_empty_dataframe(),
                    (_, true) => self.return_empty_dataframe(),
                    (_, _) => Err(DataFusionError::Execution(format!(
                        "View '{name}' doesn't exist."
                    ))),
                }
            }

            LogicalPlan::SetVariable(SetVariable {
                variable, value, ..
            }) => {
                let mut state = self.state.write();
                let config_options = &mut state.config.config_options;

                let old_value = config_options.get(&variable).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Can not SET variable: Unknown Variable {variable}"
                    ))
                })?;

                match old_value {
                    ScalarValue::Boolean(_) => {
                        let new_value = value.parse::<bool>().map_err(|_| {
                            DataFusionError::Execution(format!(
                                "Failed to parse {value} as bool",
                            ))
                        })?;
                        config_options.set_bool(&variable, new_value);
                    }

                    ScalarValue::UInt64(_) => {
                        let new_value = value.parse::<u64>().map_err(|_| {
                            DataFusionError::Execution(format!(
                                "Failed to parse {value} as u64",
                            ))
                        })?;
                        config_options.set_u64(&variable, new_value);
                    }

                    ScalarValue::Utf8(_) => {
                        let new_value = value.parse::<String>().map_err(|_| {
                            DataFusionError::Execution(format!(
                                "Failed to parse {value} as String",
                            ))
                        })?;
                        config_options.set_string(&variable, new_value);
                    }

                    _ => {
                        return Err(DataFusionError::Execution(
                            "Unsupported Scalar Value Type".to_string(),
                        ))
                    }
                }
                drop(state);

                self.return_empty_dataframe()
            }

            LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                schema_name,
                if_not_exists,
                ..
            }) => {
                // sqlparser doesnt accept database / catalog as parameter to CREATE SCHEMA
                // so for now, we default to default catalog
                let tokens: Vec<&str> = schema_name.split('.').collect();
                let (catalog, schema_name) = match tokens.len() {
                    1 => Ok((DEFAULT_CATALOG, schema_name.as_str())),
                    2 => Ok((tokens[0], tokens[1])),
                    _ => Err(DataFusionError::Execution(format!(
                        "Unable to parse catalog from {schema_name}"
                    ))),
                }?;
                let catalog = self.catalog(catalog).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Missing '{DEFAULT_CATALOG}' catalog"
                    ))
                })?;

                let schema = catalog.schema(schema_name);

                match (if_not_exists, schema) {
                    (true, Some(_)) => self.return_empty_dataframe(),
                    (true, None) | (false, None) => {
                        let schema = Arc::new(MemorySchemaProvider::new());
                        catalog.register_schema(schema_name, schema)?;
                        self.return_empty_dataframe()
                    }
                    (false, Some(_)) => Err(DataFusionError::Execution(format!(
                        "Schema '{schema_name}' already exists"
                    ))),
                }
            }
            LogicalPlan::CreateCatalog(CreateCatalog {
                catalog_name,
                if_not_exists,
                ..
            }) => {
                let catalog = self.catalog(catalog_name.as_str());

                match (if_not_exists, catalog) {
                    (true, Some(_)) => self.return_empty_dataframe(),
                    (true, None) | (false, None) => {
                        let new_catalog = Arc::new(MemoryCatalogProvider::new());
                        self.state
                            .write()
                            .catalog_list
                            .register_catalog(catalog_name, new_catalog);
                        self.return_empty_dataframe()
                    }
                    (false, Some(_)) => Err(DataFusionError::Execution(format!(
                        "Catalog '{catalog_name}' already exists"
                    ))),
                }
            }

            plan => Ok(DataFrame::new(self.state(), plan)),
        }
    }

    // return an empty dataframe
    fn return_empty_dataframe(&self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.state(), plan))
    }

    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<DataFrame> {
        let table_provider: Arc<dyn TableProvider> =
            self.create_custom_table(cmd).await?;

        let table = self.table(&cmd.name);
        match (cmd.if_not_exists, table) {
            (true, Ok(_)) => self.return_empty_dataframe(),
            (_, Err(_)) => {
                self.register_table(&cmd.name, table_provider)?;
                self.return_empty_dataframe()
            }
            (false, Ok(_)) => Err(DataFusionError::Execution(format!(
                "Table '{}' already exists",
                cmd.name
            ))),
        }
    }

    async fn create_custom_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = self.state.read().clone();
        let file_type = cmd.file_type.to_uppercase();
        let factory = &state
            .runtime_env
            .table_factories
            .get(file_type.as_str())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Unable to find factory for {}",
                    cmd.file_type
                ))
            })?;
        let table = (*factory).create(&state, cmd).await?;
        Ok(table)
    }

    fn find_and_deregister<'a>(
        &self,
        table_ref: impl Into<TableReference<'a>>,
        table_type: TableType,
    ) -> Result<bool> {
        let table_ref = table_ref.into();
        let table_provider = self
            .state
            .read()
            .schema_for_ref(table_ref)?
            .table(table_ref.table());

        if let Some(table_provider) = table_provider {
            if table_provider.table_type() == table_type {
                self.deregister_table(table_ref)?;
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Creates a logical plan.
    ///
    /// This function is intended for internal use and should not be called directly.
    #[deprecated(note = "Use SessionContext::sql which snapshots the SessionState")]
    pub fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let mut statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "The context currently only supports a single SQL statement".to_string(),
            ));
        }

        // create a query planner
        let state = self.state.read().clone();
        let query_planner = SqlToRel::new(&state);
        query_planner.statement_to_plan(statements.pop_front().unwrap())
    }

    /// Registers a variable provider within this context.
    pub fn register_variable(
        &self,
        variable_type: VarType,
        provider: Arc<dyn VarProvider + Send + Sync>,
    ) {
        self.state
            .write()
            .execution_props
            .add_var_provider(variable_type, provider);
    }

    /// Registers a scalar UDF within this context.
    ///
    /// Note in SQL queries, function names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// `SELECT MY_FUNC(x)...` will look for a function named `"my_func"`
    /// `SELECT "my_FUNC"(x)` will look for a function named `"my_FUNC"`
    pub fn register_udf(&self, f: ScalarUDF) {
        self.state
            .write()
            .scalar_functions
            .insert(f.name.clone(), Arc::new(f));
    }

    /// Registers an aggregate UDF within this context.
    ///
    /// Note in SQL queries, aggregate names are looked up using
    /// lowercase unless the query uses quotes. For example,
    ///
    /// `SELECT MY_UDAF(x)...` will look for an aggregate named `"my_udaf"`
    /// `SELECT "my_UDAF"(x)` will look for an aggregate named `"my_UDAF"`
    pub fn register_udaf(&self, f: AggregateUDF) {
        self.state
            .write()
            .aggregate_functions
            .insert(f.name.clone(), Arc::new(f));
    }

    /// Creates a [`DataFrame`] for reading an Avro data source.
    pub async fn read_avro(
        &self,
        table_path: impl AsRef<str>,
        options: AvroReadOptions<'_>,
    ) -> Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;
        let target_partitions = self.copied_config().target_partitions();

        let listing_options = options.to_listing_options(target_partitions);

        let resolved_schema = match (options.schema, options.infinite) {
            (Some(s), _) => Arc::new(s.to_owned()),
            (None, false) => {
                listing_options
                    .infer_schema(&self.state(), &table_path)
                    .await?
            }
            (None, true) => {
                return Err(DataFusionError::Plan(
                    "Schema inference for infinite data sources is not supported."
                        .to_string(),
                ))
            }
        };

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;
        self.read_table(Arc::new(provider))
    }

    /// Creates a [`DataFrame`] for reading an Json data source.
    pub async fn read_json(
        &self,
        table_path: impl AsRef<str>,
        options: NdJsonReadOptions<'_>,
    ) -> Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;
        let target_partitions = self.copied_config().target_partitions();

        let listing_options = options.to_listing_options(target_partitions);

        let resolved_schema = match (options.schema, options.infinite) {
            (Some(s), _) => Arc::new(s.to_owned()),
            (None, false) => {
                listing_options
                    .infer_schema(&self.state(), &table_path)
                    .await?
            }
            (None, true) => {
                return Err(DataFusionError::Plan(
                    "Schema inference for infinite data sources is not supported."
                        .to_string(),
                ))
            }
        };
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;

        self.read_table(Arc::new(provider))
    }

    /// Creates an empty DataFrame.
    pub fn read_empty(&self) -> Result<DataFrame> {
        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::empty(true).build()?,
        ))
    }

    /// Creates a [`DataFrame`] for reading a CSV data source.
    pub async fn read_csv(
        &self,
        table_path: impl AsRef<str>,
        options: CsvReadOptions<'_>,
    ) -> Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;
        let target_partitions = self.copied_config().target_partitions();
        let listing_options = options.to_listing_options(target_partitions);
        let resolved_schema = match (options.schema, options.infinite) {
            (Some(s), _) => Arc::new(s.to_owned()),
            (None, false) => {
                listing_options
                    .infer_schema(&self.state(), &table_path)
                    .await?
            }
            (None, true) => {
                return Err(DataFusionError::Plan(
                    "Schema inference for infinite data sources is not supported."
                        .to_string(),
                ))
            }
        };
        let config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        let provider = ListingTable::try_new(config)?;
        self.read_table(Arc::new(provider))
    }

    /// Creates a [`DataFrame`] for reading a Parquet data source.
    pub async fn read_parquet(
        &self,
        table_path: impl AsRef<str>,
        options: ParquetReadOptions<'_>,
    ) -> Result<DataFrame> {
        let table_path = ListingTableUrl::parse(table_path)?;
        let listing_options = options.to_listing_options(&self.state.read().config);

        // with parquet we resolve the schema in all cases
        let resolved_schema = listing_options
            .infer_schema(&self.state(), &table_path)
            .await?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        let provider = ListingTable::try_new(config)?;
        self.read_table(Arc::new(provider))
    }

    /// Creates a [`DataFrame`] for reading a custom [`TableProvider`].
    pub fn read_table(&self, provider: Arc<dyn TableProvider>) -> Result<DataFrame> {
        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(provider), None)?
                .build()?,
        ))
    }

    /// Creates a [`DataFrame`] for reading a [`RecordBatch`]
    pub fn read_batch(&self, batch: RecordBatch) -> Result<DataFrame> {
        let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::scan(
                UNNAMED_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        ))
    }

    /// Registers a [`ListingTable]` that can assemble multiple files
    /// from locations in an [`ObjectStore`] instance into a single
    /// table.
    ///
    /// This method is `async` because it might need to resolve the schema.
    ///
    /// [`ObjectStore`]: object_store::ObjectStore
    pub async fn register_listing_table(
        &self,
        name: &str,
        table_path: impl AsRef<str>,
        options: ListingOptions,
        provided_schema: Option<SchemaRef>,
        sql_definition: Option<String>,
    ) -> Result<()> {
        let table_path = ListingTableUrl::parse(table_path)?;
        let resolved_schema = match (provided_schema, options.infinite_source) {
            (Some(s), _) => s,
            (None, false) => options.infer_schema(&self.state(), &table_path).await?,
            (None, true) => {
                return Err(DataFusionError::Plan(
                    "Schema inference for infinite data sources is not supported."
                        .to_string(),
                ))
            }
        };
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table = ListingTable::try_new(config)?.with_definition(sql_definition);
        self.register_table(TableReference::Bare { table: name }, Arc::new(table))?;
        Ok(())
    }

    /// Registers a CSV file as a table which can referenced from SQL
    /// statements executed against this context.
    pub async fn register_csv(
        &self,
        name: &str,
        table_path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(self.copied_config().target_partitions());

        self.register_listing_table(
            name,
            table_path,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
            None,
        )
        .await?;

        Ok(())
    }

    /// Registers a Json file as a table that it can be referenced
    /// from SQL statements executed against this context.
    pub async fn register_json(
        &self,
        name: &str,
        table_path: &str,
        options: NdJsonReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(self.copied_config().target_partitions());

        self.register_listing_table(
            name,
            table_path,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
            None,
        )
        .await?;
        Ok(())
    }

    /// Registers a Parquet file as a table that can be referenced from SQL
    /// statements executed against this context.
    pub async fn register_parquet(
        &self,
        name: &str,
        table_path: &str,
        options: ParquetReadOptions<'_>,
    ) -> Result<()> {
        let listing_options = options.to_listing_options(&self.state.read().config);

        self.register_listing_table(name, table_path, listing_options, None, None)
            .await?;
        Ok(())
    }

    /// Registers an Avro file as a table that can be referenced from
    /// SQL statements executed against this context.
    pub async fn register_avro(
        &self,
        name: &str,
        table_path: &str,
        options: AvroReadOptions<'_>,
    ) -> Result<()> {
        let listing_options =
            options.to_listing_options(self.copied_config().target_partitions());

        self.register_listing_table(
            name,
            table_path,
            listing_options,
            options.schema.map(|s| Arc::new(s.to_owned())),
            None,
        )
        .await?;
        Ok(())
    }

    /// Registers a named catalog using a custom `CatalogProvider` so that
    /// it can be referenced from SQL statements executed against this
    /// context.
    ///
    /// Returns the [`CatalogProvider`] previously registered for this
    /// name, if any
    pub fn register_catalog(
        &self,
        name: impl Into<String>,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let name = name.into();
        self.state
            .read()
            .catalog_list
            .register_catalog(name, catalog)
    }

    /// Retrieves the list of available catalog names.
    pub fn catalog_names(&self) -> Vec<String> {
        self.state.read().catalog_list.catalog_names()
    }

    /// Retrieves a [`CatalogProvider`] instance by name
    pub fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.state.read().catalog_list.catalog(name)
    }

    /// Registers a [`TableProvider`] as a table that can be
    /// referenced from SQL statements executed against this context.
    ///
    /// Returns the [`TableProvider`] previously registered for this
    /// reference, if any
    pub fn register_table<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .register_table(table_ref.table().to_owned(), provider)
    }

    /// Deregisters the given table.
    ///
    /// Returns the registered provider, if any
    pub fn deregister_table<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table_ref = table_ref.into();
        self.state
            .read()
            .schema_for_ref(table_ref)?
            .deregister_table(table_ref.table())
    }

    /// Return true if the specified table exists in the schema provider.
    pub fn table_exist<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<bool> {
        let table_ref = table_ref.into();
        Ok(self
            .state
            .read()
            .schema_for_ref(table_ref)?
            .table_exist(table_ref.table()))
    }

    /// Retrieves a [`DataFrame`] representing a table previously
    /// registered by calling the [`register_table`] function.
    ///
    /// Returns an error if no table has been registered with the
    /// provided reference.
    ///
    /// [`register_table`]: SessionContext::register_table
    pub fn table<'a>(
        &self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<DataFrame> {
        let table_ref = table_ref.into();
        let provider = self.table_provider(table_ref)?;
        let plan = LogicalPlanBuilder::scan(
            table_ref.table(),
            provider_as_source(Arc::clone(&provider)),
            None,
        )?
        .build()?;
        Ok(DataFrame::new(self.state(), plan))
    }

    /// Return a [`TabelProvider`] for the specified table.
    pub fn table_provider<'a>(
        &self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_ref = table_ref.into();
        let schema = self.state.read().schema_for_ref(table_ref)?;
        match schema.table(table_ref.table()) {
            Some(ref provider) => Ok(Arc::clone(provider)),
            _ => Err(DataFusionError::Plan(format!(
                "No table named '{}'",
                table_ref.table()
            ))),
        }
    }

    /// Returns the set of available tables in the default catalog and
    /// schema.
    ///
    /// Use [`table`] to get a specific table.
    ///
    /// [`table`]: SessionContext::table
    #[deprecated(
        note = "Please use the catalog provider interface (`SessionContext::catalog`) to examine available catalogs, schemas, and tables"
    )]
    pub fn tables(&self) -> Result<HashSet<String>> {
        Ok(self
            .state
            .read()
            // a bare reference will always resolve to the default catalog and schema
            .schema_for_ref(TableReference::Bare { table: "" })?
            .table_names()
            .iter()
            .cloned()
            .collect())
    }

    /// Optimizes the logical plan by applying optimizer rules.
    #[deprecated(
        note = "Use SessionState::optimize to ensure a consistent state for planning and execution"
    )]
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.state.read().optimize(plan)
    }

    /// Creates a physical plan from a logical plan.
    #[deprecated(
        note = "Use SessionState::create_physical_plan or DataFrame::create_physical_plan to ensure a consistent state for planning and execution"
    )]
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.state().create_physical_plan(logical_plan).await
    }

    /// Executes a query and writes the results to a partitioned CSV file.
    pub async fn write_csv(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        let state = self.state.read().clone();
        plan_to_csv(&state, plan, path).await
    }

    /// Executes a query and writes the results to a partitioned JSON file.
    pub async fn write_json(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
    ) -> Result<()> {
        let state = self.state.read().clone();
        plan_to_json(&state, plan, path).await
    }

    /// Executes a query and writes the results to a partitioned Parquet file.
    pub async fn write_parquet(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: impl AsRef<str>,
        writer_properties: Option<WriterProperties>,
    ) -> Result<()> {
        let state = self.state.read().clone();
        plan_to_parquet(&state, plan, path, writer_properties).await
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
    }

    /// Snapshots the [`SessionState`] of this [`SessionContext`] setting the
    /// `query_execution_start_time` to the current time
    pub fn state(&self) -> SessionState {
        let mut state = self.state.read().clone();
        state.execution_props.start_execution();
        state
    }
}

impl FunctionRegistry for SessionContext {
    fn udfs(&self) -> HashSet<String> {
        self.state.read().udfs()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.state.read().udf(name)
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.state.read().udaf(name)
    }
}

/// A planner used to add extensions to DataFusion logical and physical plans.
#[async_trait]
pub trait QueryPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// The query planner used if no user defined planner is provided
struct DefaultQueryPlanner {}

#[async_trait]
impl QueryPlanner for DefaultQueryPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::default();
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Map that holds opaque objects indexed by their type.
///
/// Data is wrapped into an [`Arc`] to enable [`Clone`] while still being [object safe].
///
/// [object safe]: https://doc.rust-lang.org/reference/items/traits.html#object-safety
type AnyMap =
    HashMap<TypeId, Arc<dyn Any + Send + Sync + 'static>, BuildHasherDefault<IdHasher>>;

/// Hasher for [`AnyMap`].
///
/// With [`TypeId`}s as keys, there's no need to hash them. They are already hashes themselves, coming from the compiler.
/// The [`IdHasher`} just holds the [`u64`} of the [`TypeId`}, and then returns it, instead of doing any bit fiddling.
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {
    fn write(&mut self, _: &[u8]) {
        unreachable!("TypeId calls write_u64");
    }

    #[inline]
    fn write_u64(&mut self, id: u64) {
        self.0 = id;
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

/// Configuration options for session context
#[derive(Clone)]
pub struct SessionConfig {
    /// Default catalog name for table resolution
    default_catalog: String,
    /// Default schema name for table resolution (not in ConfigOptions
    /// due to `resolve_table_ref` which passes back references)
    default_schema: String,
    /// Configuration options
    config_options: ConfigOptions,
    /// Opaque extensions.
    extensions: AnyMap,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            default_catalog: DEFAULT_CATALOG.to_owned(),
            default_schema: DEFAULT_SCHEMA.to_owned(),
            config_options: ConfigOptions::new(),
            // Assume no extensions by default.
            extensions: HashMap::with_capacity_and_hasher(
                0,
                BuildHasherDefault::default(),
            ),
        }
    }
}

impl SessionConfig {
    /// Create an execution config with default setting
    pub fn new() -> Self {
        Default::default()
    }

    /// Create an execution config with config options read from the environment
    pub fn from_env() -> Self {
        Self {
            config_options: ConfigOptions::from_env(),
            ..Default::default()
        }
    }

    /// Set a configuration option
    pub fn set(mut self, key: &str, value: ScalarValue) -> Self {
        self.config_options.set(key, value);
        self
    }

    /// Set a boolean configuration option
    pub fn set_bool(self, key: &str, value: bool) -> Self {
        self.set(key, ScalarValue::Boolean(Some(value)))
    }

    /// Set a generic `u64` configuration option
    pub fn set_u64(self, key: &str, value: u64) -> Self {
        self.set(key, ScalarValue::UInt64(Some(value)))
    }

    /// Set a generic `usize` configuration option
    pub fn set_usize(self, key: &str, value: usize) -> Self {
        let value: u64 = value.try_into().expect("convert usize to u64");
        self.set(key, ScalarValue::UInt64(Some(value)))
    }

    /// Set a generic `str` configuration option
    pub fn set_str(self, key: &str, value: &str) -> Self {
        self.set(key, ScalarValue::Utf8(Some(value.to_string())))
    }

    /// Customize batch size
    pub fn with_batch_size(self, n: usize) -> Self {
        // batch size must be greater than zero
        assert!(n > 0);
        self.set_u64(OPT_BATCH_SIZE, n.try_into().unwrap())
    }

    /// Customize [`OPT_TARGET_PARTITIONS`]
    pub fn with_target_partitions(self, n: usize) -> Self {
        // partition count must be greater than zero
        assert!(n > 0);
        self.set_usize(OPT_TARGET_PARTITIONS, n)
    }

    /// get target_partitions
    pub fn target_partitions(&self) -> usize {
        self.config_options
            .get_usize(OPT_TARGET_PARTITIONS)
            .expect("target partitions must be set")
    }

    /// Is the information schema enabled?
    pub fn information_schema(&self) -> bool {
        self.config_options
            .get_bool(OPT_INFORMATION_SCHEMA)
            .unwrap_or_default()
    }

    /// Should the context create the default catalog and schema?
    pub fn create_default_catalog_and_schema(&self) -> bool {
        self.config_options
            .get_bool(OPT_CREATE_DEFAULT_CATALOG_AND_SCHEMA)
            .unwrap_or_default()
    }

    /// Are joins repartitioned during execution?
    pub fn repartition_joins(&self) -> bool {
        self.config_options
            .get_bool(OPT_REPARTITION_JOINS)
            .unwrap_or_default()
    }

    /// Are aggregates repartitioned during execution?
    pub fn repartition_aggregations(&self) -> bool {
        self.config_options
            .get_bool(OPT_REPARTITION_AGGREGATIONS)
            .unwrap_or_default()
    }

    /// Are window functions repartitioned during execution?
    pub fn repartition_window_functions(&self) -> bool {
        self.config_options
            .get_bool(OPT_REPARTITION_WINDOWS)
            .unwrap_or_default()
    }

    /// Are statistics collected during execution?
    pub fn collect_statistics(&self) -> bool {
        self.config_options
            .get_bool(OPT_COLLECT_STATISTICS)
            .unwrap_or_default()
    }

    /// Selects a name for the default catalog and schema
    pub fn with_default_catalog_and_schema(
        mut self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
    ) -> Self {
        self.default_catalog = catalog.into();
        self.default_schema = schema.into();
        self
    }

    /// Controls whether the default catalog and schema will be automatically created
    pub fn with_create_default_catalog_and_schema(mut self, create: bool) -> Self {
        self.config_options
            .set_bool(OPT_CREATE_DEFAULT_CATALOG_AND_SCHEMA, create);
        self
    }

    /// Enables or disables the inclusion of `information_schema` virtual tables
    pub fn with_information_schema(mut self, enabled: bool) -> Self {
        self.config_options
            .set_bool(OPT_INFORMATION_SCHEMA, enabled);
        self
    }

    /// Enables or disables the use of repartitioning for joins to improve parallelism
    pub fn with_repartition_joins(mut self, enabled: bool) -> Self {
        self.config_options.set_bool(OPT_REPARTITION_JOINS, enabled);
        self
    }

    /// Enables or disables the use of repartitioning for aggregations to improve parallelism
    pub fn with_repartition_aggregations(mut self, enabled: bool) -> Self {
        self.config_options
            .set_bool(OPT_REPARTITION_AGGREGATIONS, enabled);
        self
    }

    /// Enables or disables the use of repartitioning for window functions to improve parallelism
    pub fn with_repartition_windows(mut self, enabled: bool) -> Self {
        self.config_options
            .set_bool(OPT_REPARTITION_WINDOWS, enabled);
        self
    }

    /// Enables or disables the use of pruning predicate for parquet readers to skip row groups
    pub fn with_parquet_pruning(mut self, enabled: bool) -> Self {
        self.config_options
            .set_bool(OPT_PARQUET_ENABLE_PRUNING, enabled);
        self
    }

    /// Returns true if pruning predicate should be used to skip parquet row groups
    pub fn parquet_pruning(&self) -> bool {
        self.config_options
            .get_bool(OPT_PARQUET_ENABLE_PRUNING)
            .unwrap_or(false)
    }

    /// Enables or disables the collection of statistics after listing files
    pub fn with_collect_statistics(mut self, enabled: bool) -> Self {
        self.config_options
            .set_bool(OPT_COLLECT_STATISTICS, enabled);
        self
    }

    /// Get the currently configured batch size
    pub fn batch_size(&self) -> usize {
        self.config_options
            .get_u64(OPT_BATCH_SIZE)
            .unwrap_or_default()
            .try_into()
            .unwrap()
    }

    /// Enables or disables the coalescence of small batches into larger batches
    pub fn with_coalesce_batches(mut self, enabled: bool) -> Self {
        self.config_options.set_bool(OPT_COALESCE_BATCHES, enabled);
        self
    }

    /// Returns true if record batches will be examined between each operator
    /// and small batches will be coalesced into larger batches.
    pub fn coalesce_batches(&self) -> bool {
        self.config_options
            .get_bool(OPT_COALESCE_BATCHES)
            .unwrap_or_default()
    }

    /// Enables or disables the round robin repartition for increasing parallelism
    pub fn with_round_robin_repartition(mut self, enabled: bool) -> Self {
        self.config_options
            .set_bool(OPT_ENABLE_ROUND_ROBIN_REPARTITION, enabled);
        self
    }

    /// Returns true if the physical plan optimizer will try to
    /// add round robin repartition to increase parallelism to leverage more CPU cores.
    pub fn round_robin_repartition(&self) -> bool {
        self.config_options
            .get_bool(OPT_ENABLE_ROUND_ROBIN_REPARTITION)
            .unwrap_or_default()
    }

    /// Return a handle to the configuration options.
    ///
    /// [`config_options`]: SessionContext::config_option
    pub fn config_options(&self) -> &ConfigOptions {
        &self.config_options
    }

    /// Return a mutable handle to the configuration options.
    ///
    /// [`config_options`]: SessionContext::config_option
    pub fn config_options_mut(&mut self) -> &mut ConfigOptions {
        &mut self.config_options
    }

    /// Add extensions.
    ///
    /// Extensions can be used to attach extra data to the session config -- e.g. tracing information or caches.
    /// Extensions are opaque and the types are unknown to DataFusion itself, which makes them extremely flexible. [^1]
    ///
    /// Extensions are stored within an [`Arc`] so they do NOT require [`Clone`]. The are immutable. If you need to
    /// modify their state over their lifetime -- e.g. for caches -- you need to establish some for of interior mutability.
    ///
    /// Extensions are indexed by their type `T`. If multiple values of the same type are provided, only the last one
    /// will be kept.
    ///
    /// You may use [`get_extension`](Self::get_extension) to retrieve extensions.
    ///
    /// # Example
    /// ```
    /// use std::sync::Arc;
    /// use datafusion::execution::context::SessionConfig;
    ///
    /// // application-specific extension types
    /// struct Ext1(u8);
    /// struct Ext2(u8);
    /// struct Ext3(u8);
    ///
    /// let ext1a = Arc::new(Ext1(10));
    /// let ext1b = Arc::new(Ext1(11));
    /// let ext2 = Arc::new(Ext2(2));
    ///
    /// let cfg = SessionConfig::default()
    ///     // will only remember the last Ext1
    ///     .with_extension(Arc::clone(&ext1a))
    ///     .with_extension(Arc::clone(&ext1b))
    ///     .with_extension(Arc::clone(&ext2));
    ///
    /// let ext1_received = cfg.get_extension::<Ext1>().unwrap();
    /// assert!(!Arc::ptr_eq(&ext1_received, &ext1a));
    /// assert!(Arc::ptr_eq(&ext1_received, &ext1b));
    ///
    /// let ext2_received = cfg.get_extension::<Ext2>().unwrap();
    /// assert!(Arc::ptr_eq(&ext2_received, &ext2));
    ///
    /// assert!(cfg.get_extension::<Ext3>().is_none());
    /// ```
    ///
    /// [^1]: Compare that to [`ConfigOptions`] which only supports [`ScalarValue`] payloads.
    pub fn with_extension<T>(mut self, ext: Arc<T>) -> Self
    where
        T: Send + Sync + 'static,
    {
        let ext = ext as Arc<dyn Any + Send + Sync + 'static>;
        let id = TypeId::of::<T>();
        self.extensions.insert(id, ext);
        self
    }

    /// Get extension, if any for the specified type `T` exists.
    ///
    /// See [`with_extension`](Self::with_extension) on how to add attach extensions.
    pub fn get_extension<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        let id = TypeId::of::<T>();
        self.extensions
            .get(&id)
            .cloned()
            .map(|ext| Arc::downcast(ext).expect("TypeId unique"))
    }
}

/// Execution context for registering data sources and executing queries
#[derive(Clone)]
pub struct SessionState {
    /// Uuid for the session
    session_id: String,
    /// Responsible for optimizing a logical plan
    optimizer: Optimizer,
    /// Responsible for optimizing a physical execution plan
    physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    /// Responsible for planning `LogicalPlan`s, and `ExecutionPlan`
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    /// Collection of catalogs containing schemas and ultimately TableProviders
    catalog_list: Arc<dyn CatalogList>,
    /// Scalar functions that are registered with the context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions registered in the context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Session configuration
    config: SessionConfig,
    /// Execution properties
    execution_props: ExecutionProps,
    /// Runtime environment
    runtime_env: Arc<RuntimeEnv>,
}

impl Debug for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionState")
            .field("session_id", &self.session_id)
            // TODO should we print out more?
            .finish()
    }
}

/// Default session builder using the provided configuration
pub fn default_session_builder(config: SessionConfig) -> SessionState {
    SessionState::with_config_rt(config, Arc::new(RuntimeEnv::default()))
}

impl SessionState {
    /// Returns new SessionState using the provided configuration and runtime
    pub fn with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let session_id = Uuid::new_v4().to_string();

        let catalog_list = Arc::new(MemoryCatalogList::new()) as Arc<dyn CatalogList>;
        if config.create_default_catalog_and_schema() {
            let default_catalog = MemoryCatalogProvider::new();

            default_catalog
                .register_schema(
                    &config.default_schema,
                    Arc::new(MemorySchemaProvider::new()),
                )
                .expect("memory catalog provider can register schema");

            Self::register_default_schema(&config, &runtime, &default_catalog);

            catalog_list.register_catalog(
                config.default_catalog.clone(),
                Arc::new(default_catalog),
            );
        }

        // We need to take care of the rule ordering. They may influence each other.
        let mut physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> =
            vec![Arc::new(AggregateStatistics::new())];
        // - In order to increase the parallelism, it will change the output partitioning
        // of some operators in the plan tree, which will influence other rules.
        // Therefore, it should be run as soon as possible.
        // - The reason to make it optional is
        //      - it's not used for the distributed engine, Ballista.
        //      - it's conflicted with some parts of the BasicEnforcement, since it will
        //      introduce additional repartitioning while the BasicEnforcement aims at
        //      reducing unnecessary repartitioning.
        if config.round_robin_repartition() {
            physical_optimizers.push(Arc::new(Repartition::new()));
        }
        //- Currently it will depend on the partition number to decide whether to change the
        // single node sort to parallel local sort and merge. Therefore, it should be run
        // after the Repartition.
        // - Since it will change the output ordering of some operators, it should be run
        // before JoinSelection and BasicEnforcement, which may depend on that.
        physical_optimizers.push(Arc::new(GlobalSortSelection::new()));
        // Statistics-base join selection will change the Auto mode to real join implementation,
        // like collect left, or hash join, or future sort merge join, which will
        // influence the BasicEnforcement to decide whether to add additional repartition
        // and local sort to meet the distribution and ordering requirements.
        // Therefore, it should be run before BasicEnforcement
        physical_optimizers.push(Arc::new(JoinSelection::new()));
        // If the query is processing infinite inputs, the PipelineFixer rule applies the
        // necessary transformations to make the query runnable (if it is not already runnable).
        // If the query can not be made runnable, the rule emits an error with a diagnostic message.
        // Since the transformations it applies may alter output partitioning properties of operators
        // (e.g. by swapping hash join sides), this rule runs before BasicEnforcement.
        physical_optimizers.push(Arc::new(PipelineFixer::new()));
        // It's for adding essential repartition and local sorting operator to satisfy the
        // required distribution and local sort.
        // Please make sure that the whole plan tree is determined.
        physical_optimizers.push(Arc::new(EnforceDistribution::new()));
        // `BasicEnforcement` stage conservatively inserts `SortExec`s to satisfy ordering requirements.
        // However, a deeper analysis may sometimes reveal that such a `SortExec` is actually unnecessary.
        // These cases typically arise when we have reversible `WindowAggExec`s or deep subqueries. The
        // rule below performs this analysis and removes unnecessary `SortExec`s.
        physical_optimizers.push(Arc::new(EnforceSorting::new()));
        // It will not influence the distribution and ordering of the whole plan tree.
        // Therefore, to avoid influencing other rules, it should be run at last.
        if config.coalesce_batches() {
            physical_optimizers.push(Arc::new(CoalesceBatches::new(config.batch_size())));
        }
        // The PipelineChecker rule will reject non-runnable query plans that use
        // pipeline-breaking operators on infinite input(s). The rule generates a
        // diagnostic error message when this happens. It makes no changes to the
        // given query plan; i.e. it only acts as a final gatekeeping rule.
        physical_optimizers.push(Arc::new(PipelineChecker::new()));
        SessionState {
            session_id,
            optimizer: Optimizer::new(),
            physical_optimizers,
            query_planner: Arc::new(DefaultQueryPlanner {}),
            catalog_list,
            scalar_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            config,
            execution_props: ExecutionProps::new(),
            runtime_env: runtime,
        }
    }

    fn register_default_schema(
        config: &SessionConfig,
        runtime: &Arc<RuntimeEnv>,
        default_catalog: &MemoryCatalogProvider,
    ) {
        let url = config.config_options.get("datafusion.catalog.location");
        let format = config.config_options.get("datafusion.catalog.type");
        let (url, format) = match (url, format) {
            (Some(url), Some(format)) => (url, format),
            _ => return,
        };
        if url.is_null() || format.is_null() {
            return;
        }
        let url = url.to_string();
        let format = format.to_string();

        let has_header = config.config_options.get("datafusion.catalog.has_header");
        let has_header: bool = has_header
            .map(|x| FromStr::from_str(&x.to_string()).unwrap_or_default())
            .unwrap_or_default();

        let url = Url::parse(url.as_str()).expect("Invalid default catalog location!");
        let authority = match url.host_str() {
            Some(host) => format!("{}://{}", url.scheme(), host),
            None => format!("{}://", url.scheme()),
        };
        let path = &url.as_str()[authority.len()..];
        let path = object_store::path::Path::parse(path).expect("Can't parse path");
        let store = ObjectStoreUrl::parse(authority.as_str())
            .expect("Invalid default catalog url");
        let store = match runtime.object_store(store) {
            Ok(store) => store,
            _ => return,
        };
        let factory = match runtime.table_factories.get(format.as_str()) {
            Some(factory) => factory,
            _ => return,
        };
        let schema = ListingSchemaProvider::new(
            authority,
            path,
            factory.clone(),
            store,
            format,
            has_header,
        );
        let _ = default_catalog
            .register_schema("default", Arc::new(schema))
            .expect("Failed to register default schema");
    }

    fn resolve_table_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> ResolvedTableReference<'a> {
        table_ref
            .into()
            .resolve(&self.config.default_catalog, &self.config.default_schema)
    }

    fn schema_for_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let resolved_ref = self.resolve_table_ref(table_ref);
        if self.config.information_schema() && resolved_ref.schema == INFORMATION_SCHEMA {
            return Ok(Arc::new(InformationSchemaProvider::new(
                self.catalog_list.clone(),
            )));
        }

        self.catalog_list
            .catalog(resolved_ref.catalog)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve catalog: {}",
                    resolved_ref.catalog
                ))
            })?
            .schema(resolved_ref.schema)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve schema: {}",
                    resolved_ref.schema
                ))
            })
    }

    /// Replace the default query planner
    pub fn with_query_planner(
        mut self,
        query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Self {
        self.query_planner = query_planner;
        self
    }

    /// Replace the optimizer rules
    pub fn with_optimizer_rules(
        mut self,
        rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    ) -> Self {
        self.optimizer = Optimizer::with_rules(rules);
        self
    }

    /// Replace the physical optimizer rules
    pub fn with_physical_optimizer_rules(
        mut self,
        physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    ) -> Self {
        self.physical_optimizers = physical_optimizers;
        self
    }

    /// Adds a new [`OptimizerRule`]
    pub fn add_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Self {
        self.optimizer.rules.push(optimizer_rule);
        self
    }

    /// Adds a new [`PhysicalOptimizerRule`]
    pub fn add_physical_optimizer_rule(
        mut self,
        optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) -> Self {
        self.physical_optimizers.push(optimizer_rule);
        self
    }

    /// Optimizes the logical plan by applying optimizer rules.
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // TODO: Implement OptimizerContext directly on DataFrame (#4631) (#4626)
        let config = {
            let config_options = self.config_options();
            OptimizerContext::new()
                .with_skip_failing_rules(
                    config_options
                        .get_bool(OPT_OPTIMIZER_SKIP_FAILED_RULES)
                        .unwrap_or_default(),
                )
                .with_max_passes(
                    config_options
                        .get_u64(OPT_OPTIMIZER_MAX_PASSES)
                        .unwrap_or_default() as u8,
                )
                .with_query_execution_start_time(
                    self.execution_props.query_execution_start_time,
                )
                .filter_null_keys(
                    config_options
                        .get_bool(OPT_FILTER_NULL_JOIN_KEYS)
                        .unwrap_or_default(),
                )
        };

        if let LogicalPlan::Explain(e) = plan {
            let mut stringified_plans = e.stringified_plans.clone();

            // optimize the child plan, capturing the output of each optimizer
            let plan = self.optimizer.optimize(
                e.plan.as_ref(),
                &config,
                |optimized_plan, optimizer| {
                    let optimizer_name = optimizer.name().to_string();
                    let plan_type = PlanType::OptimizedLogicalPlan { optimizer_name };
                    stringified_plans.push(optimized_plan.to_stringified(plan_type));
                },
            )?;

            Ok(LogicalPlan::Explain(Explain {
                verbose: e.verbose,
                plan: Arc::new(plan),
                stringified_plans,
                schema: e.schema.clone(),
            }))
        } else {
            self.optimizer.optimize(plan, &config, |_, _| {})
        }
    }

    /// Creates a physical plan from a logical plan.
    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.optimize(logical_plan)?;
        self.query_planner
            .create_physical_plan(&logical_plan, self)
            .await
    }

    /// Return the session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Return the runtime env
    pub fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    /// Return the execution properties
    pub fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }

    /// Return the [`SessionConfig`]
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }

    /// Return the physical optimizers
    pub fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.physical_optimizers
    }

    /// return the configuration options
    pub fn config_options(&self) -> &ConfigOptions {
        self.config.config_options()
    }

    /// Get a new TaskContext to run in this session
    pub fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
    }
}

impl ContextProvider for SessionState {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let resolved_ref = self.resolve_table_ref(name);
        match self.schema_for_ref(resolved_ref) {
            Ok(schema) => {
                let provider = schema.table(resolved_ref.table).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "table '{}.{}.{}' not found",
                        resolved_ref.catalog, resolved_ref.schema, resolved_ref.table
                    ))
                })?;
                Ok(provider_as_source(provider))
            }
            Err(e) => Err(e),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate_functions.get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let provider_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.execution_props
            .var_providers
            .as_ref()
            .and_then(|provider| provider.get(&provider_type)?.get_type(variable_names))
    }

    fn get_config_option(&self, variable: &str) -> Option<ScalarValue> {
        self.config_options().get(variable)
    }
}

impl FunctionRegistry for SessionState {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "There is no UDF named \"{name}\" in the registry"
            ))
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "There is no UDAF named \"{name}\" in the registry"
            ))
        })
    }
}

/// Task Execution Context
pub struct TaskContext {
    /// Session Id
    session_id: String,
    /// Optional Task Identify
    task_id: Option<String>,
    /// Session configuration
    session_config: SessionConfig,
    /// Scalar functions associated with this task context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions associated with this task context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Runtime environment associated with this task context
    runtime: Arc<RuntimeEnv>,
}

impl TaskContext {
    /// Create a new task context instance
    pub fn new(
        task_id: String,
        session_id: String,
        task_props: HashMap<String, String>,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        let session_config = if task_props.is_empty() {
            SessionConfig::new()
        } else {
            SessionConfig::new()
                .with_batch_size(task_props.get(OPT_BATCH_SIZE).unwrap().parse().unwrap())
                .with_target_partitions(
                    task_props
                        .get(OPT_TARGET_PARTITIONS)
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .with_repartition_joins(
                    task_props
                        .get(OPT_REPARTITION_JOINS)
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .with_repartition_aggregations(
                    task_props
                        .get(OPT_REPARTITION_AGGREGATIONS)
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .with_repartition_windows(
                    task_props
                        .get(OPT_REPARTITION_WINDOWS)
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .with_parquet_pruning(
                    task_props
                        .get(OPT_PARQUET_ENABLE_PRUNING)
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
                .with_collect_statistics(
                    task_props
                        .get(OPT_COLLECT_STATISTICS)
                        .unwrap()
                        .parse()
                        .unwrap(),
                )
        };

        Self {
            task_id: Some(task_id),
            session_id,
            session_config,
            scalar_functions,
            aggregate_functions,
            runtime,
        }
    }

    /// Return the SessionConfig associated with the Task
    pub fn session_config(&self) -> &SessionConfig {
        &self.session_config
    }

    /// Return the session_id of this [TaskContext]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return the task_id of this [TaskContext]
    pub fn task_id(&self) -> Option<String> {
        self.task_id.clone()
    }

    /// Return the [`MemoryPool`] associated with this [TaskContext]
    pub fn memory_pool(&self) -> &Arc<dyn MemoryPool> {
        &self.runtime.memory_pool
    }

    /// Return the [RuntimeEnv] associated with this [TaskContext]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime.clone()
    }
}

/// Create a new task context instance from SessionContext
impl From<&SessionContext> for TaskContext {
    fn from(session: &SessionContext) -> Self {
        TaskContext::from(&*session.state.read())
    }
}

/// Create a new task context instance from SessionState
impl From<&SessionState> for TaskContext {
    fn from(state: &SessionState) -> Self {
        let session_id = state.session_id.clone();
        let session_config = state.config.clone();
        let scalar_functions = state.scalar_functions.clone();
        let aggregate_functions = state.aggregate_functions.clone();
        let runtime = state.runtime_env.clone();
        Self {
            task_id: None,
            session_id,
            session_config,
            scalar_functions,
            aggregate_functions,
            runtime,
        }
    }
}

impl FunctionRegistry for TaskContext {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDF named \"{name}\" in the TaskContext"
            ))
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDAF named \"{name}\" in the TaskContext"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_batches_eq;
    use crate::execution::context::QueryPlanner;
    use crate::execution::memory_pool::MemoryConsumer;
    use crate::execution::runtime_env::RuntimeConfig;
    use crate::physical_plan::expressions::AvgAccumulator;
    use crate::test;
    use crate::test_util::parquet_test_data;
    use crate::variable::VarType;
    use arrow::array::ArrayRef;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use datafusion_expr::{create_udaf, create_udf, Expr, Volatility};
    use datafusion_physical_expr::functions::make_scalar_function;
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Weak;
    use std::{env, io::prelude::*};
    use tempfile::TempDir;

    #[tokio::test]
    async fn shared_memory_and_disk_manager() {
        // Demonstrate the ability to share DiskManager and
        // MemoryPool between two different executions.
        let ctx1 = SessionContext::new();

        // configure with same memory / disk manager
        let memory_pool = ctx1.runtime_env().memory_pool.clone();

        let mut reservation = MemoryConsumer::new("test").register(&memory_pool);
        reservation.grow(100);

        let disk_manager = ctx1.runtime_env().disk_manager.clone();

        let ctx2 =
            SessionContext::with_config_rt(SessionConfig::new(), ctx1.runtime_env());

        assert_eq!(ctx1.runtime_env().memory_pool.reserved(), 100);
        assert_eq!(ctx2.runtime_env().memory_pool.reserved(), 100);

        drop(reservation);

        assert_eq!(ctx1.runtime_env().memory_pool.reserved(), 0);
        assert_eq!(ctx2.runtime_env().memory_pool.reserved(), 0);

        assert!(std::ptr::eq(
            Arc::as_ptr(&disk_manager),
            Arc::as_ptr(&ctx1.runtime_env().disk_manager)
        ));
        assert!(std::ptr::eq(
            Arc::as_ptr(&disk_manager),
            Arc::as_ptr(&ctx2.runtime_env().disk_manager)
        ));
    }

    #[tokio::test]
    async fn create_variable_expr() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let ctx = create_ctx(&tmp_dir, partition_count).await?;

        let variable_provider = test::variable::SystemVar::new();
        ctx.register_variable(VarType::System, Arc::new(variable_provider));
        let variable_provider = test::variable::UserDefinedVar::new();
        ctx.register_variable(VarType::UserDefined, Arc::new(variable_provider));

        let provider = test::create_table_dual();
        ctx.register_table("dual", provider)?;

        let results =
            plan_and_collect(&ctx, "SELECT @@version, @name, @integer + 1 FROM dual")
                .await?;

        let expected = vec![
            "+----------------------+------------------------+---------------------+",
            "| @@version            | @name                  | @integer + Int64(1) |",
            "+----------------------+------------------------+---------------------+",
            "| system-var-@@version | user-defined-var-@name | 42                  |",
            "+----------------------+------------------------+---------------------+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn create_variable_err() -> Result<()> {
        let ctx = SessionContext::new();

        let err = plan_and_collect(&ctx, "SElECT @=   X#=?!~ 5")
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Execution error: variable [\"@\"] has no type information"
        );
        Ok(())
    }

    #[tokio::test]
    async fn register_deregister() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let ctx = create_ctx(&tmp_dir, partition_count).await?;

        let provider = test::create_table_dual();
        ctx.register_table("dual", provider)?;

        assert!(ctx.deregister_table("dual")?.is_some());
        assert!(ctx.deregister_table("dual")?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn case_sensitive_identifiers_user_defined_functions() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_table("t", test::table_with_sequence(1, 1).unwrap())
            .unwrap();

        let myfunc = |args: &[ArrayRef]| Ok(Arc::clone(&args[0]));
        let myfunc = make_scalar_function(myfunc);

        ctx.register_udf(create_udf(
            "MY_FUNC",
            vec![DataType::Int32],
            Arc::new(DataType::Int32),
            Volatility::Immutable,
            myfunc,
        ));

        // doesn't work as it was registered with non lowercase
        let err = plan_and_collect(&ctx, "SELECT MY_FUNC(i) FROM t")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Invalid function \'my_func\'"
        );

        // Can call it if you put quotes
        let result = plan_and_collect(&ctx, "SELECT \"MY_FUNC\"(i) FROM t").await?;

        let expected = vec![
            "+--------------+",
            "| MY_FUNC(t.i) |",
            "+--------------+",
            "| 1            |",
            "+--------------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn case_sensitive_identifiers_user_defined_aggregates() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_table("t", test::table_with_sequence(1, 1).unwrap())
            .unwrap();

        // Note capitalization
        let my_avg = create_udaf(
            "MY_AVG",
            DataType::Float64,
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            Arc::new(|_| Ok(Box::new(AvgAccumulator::try_new(&DataType::Float64)?))),
            Arc::new(vec![DataType::UInt64, DataType::Float64]),
        );

        ctx.register_udaf(my_avg);

        // doesn't work as it was registered as non lowercase
        let err = plan_and_collect(&ctx, "SELECT MY_AVG(i) FROM t")
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error during planning: Invalid function \'my_avg\'"
        );

        // Can call it if you put quotes
        let result = plan_and_collect(&ctx, "SELECT \"MY_AVG\"(i) FROM t").await?;

        let expected = vec![
            "+-------------+",
            "| MY_AVG(t.i) |",
            "+-------------+",
            "| 1           |",
            "+-------------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn query_csv_with_custom_partition_extension() -> Result<()> {
        let tmp_dir = TempDir::new()?;

        // The main stipulation of this test: use a file extension that isn't .csv.
        let file_extension = ".tst";

        let ctx = SessionContext::new();
        let schema = populate_csv_partitions(&tmp_dir, 2, file_extension)?;
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new()
                .schema(&schema)
                .file_extension(file_extension),
        )
        .await?;
        let results =
            plan_and_collect(&ctx, "SELECT SUM(c1), SUM(c2), COUNT(*) FROM test").await?;

        assert_eq!(results.len(), 1);
        let expected = vec![
            "+--------------+--------------+-----------------+",
            "| SUM(test.c1) | SUM(test.c2) | COUNT(UInt8(1)) |",
            "+--------------+--------------+-----------------+",
            "| 10           | 110          | 20              |",
            "+--------------+--------------+-----------------+",
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }

    #[tokio::test]
    async fn send_context_to_threads() -> Result<()> {
        // ensure SessionContexts can be used in a multi-threaded
        // environment. Usecase is for concurrent planing.
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let ctx = Arc::new(create_ctx(&tmp_dir, partition_count).await?);

        let threads: Vec<_> = (0..2)
            .map(|_| ctx.clone())
            .map(|ctx| {
                tokio::spawn(async move {
                    // Ensure we can create logical plan code on a separate thread.
                    ctx.sql("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")
                        .await
                })
            })
            .collect();

        for handle in threads {
            handle.await.unwrap().unwrap();
        }
        Ok(())
    }

    #[tokio::test]
    async fn with_listing_schema_provider() -> Result<()> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let path = path.join("tests/tpch-csv");
        let url = format!("file://{}", path.display());

        let rt_cfg = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(rt_cfg).unwrap());
        let cfg = SessionConfig::new()
            .set_str("datafusion.catalog.location", url.as_str())
            .set_str("datafusion.catalog.type", "CSV")
            .set_str("datafusion.catalog.has_header", "true");
        let session_state = SessionState::with_config_rt(cfg, runtime);
        let ctx = SessionContext::with_state(session_state);
        ctx.refresh_catalogs().await?;

        let result =
            plan_and_collect(&ctx, "select c_name from default.customer limit 3;")
                .await?;

        let actual = arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();
        let expected = r#"+--------------------+
| c_name             |
+--------------------+
| Customer#000000002 |
| Customer#000000003 |
| Customer#000000004 |
+--------------------+"#;
        assert_eq!(actual, expected);

        Ok(())
    }

    #[tokio::test]
    async fn custom_query_planner() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let session_state = SessionState::with_config_rt(SessionConfig::new(), runtime)
            .with_query_planner(Arc::new(MyQueryPlanner {}));
        let ctx = SessionContext::with_state(session_state);

        let df = ctx.sql("SELECT 1").await?;
        df.collect().await.expect_err("query not supported");
        Ok(())
    }

    #[tokio::test]
    async fn disabled_default_catalog_and_schema() -> Result<()> {
        let ctx = SessionContext::with_config(
            SessionConfig::new().with_create_default_catalog_and_schema(false),
        );

        assert!(matches!(
            ctx.register_table("test", test::table_with_sequence(1, 1)?),
            Err(DataFusionError::Plan(_))
        ));

        assert!(matches!(
            ctx.sql("select * from datafusion.public.test").await,
            Err(DataFusionError::Plan(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn custom_catalog_and_schema() {
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_default_catalog_and_schema("my_catalog", "my_schema");
        catalog_and_schema_test(config).await;
    }

    #[tokio::test]
    async fn custom_catalog_and_schema_no_default() {
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(false)
            .with_default_catalog_and_schema("my_catalog", "my_schema");
        catalog_and_schema_test(config).await;
    }

    #[tokio::test]
    async fn custom_catalog_and_schema_and_information_schema() {
        let config = SessionConfig::new()
            .with_create_default_catalog_and_schema(true)
            .with_information_schema(true)
            .with_default_catalog_and_schema("my_catalog", "my_schema");
        catalog_and_schema_test(config).await;
    }

    async fn catalog_and_schema_test(config: SessionConfig) {
        let ctx = SessionContext::with_config(config);
        let catalog = MemoryCatalogProvider::new();
        let schema = MemorySchemaProvider::new();
        schema
            .register_table("test".to_owned(), test::table_with_sequence(1, 1).unwrap())
            .unwrap();
        catalog
            .register_schema("my_schema", Arc::new(schema))
            .unwrap();
        ctx.register_catalog("my_catalog", Arc::new(catalog));

        for table_ref in &["my_catalog.my_schema.test", "my_schema.test", "test"] {
            let result = plan_and_collect(
                &ctx,
                &format!("SELECT COUNT(*) AS count FROM {table_ref}"),
            )
            .await
            .unwrap();

            let expected = vec![
                "+-------+",
                "| count |",
                "+-------+",
                "| 1     |",
                "+-------+",
            ];
            assert_batches_eq!(expected, &result);
        }
    }

    #[tokio::test]
    async fn cross_catalog_access() -> Result<()> {
        let ctx = SessionContext::new();

        let catalog_a = MemoryCatalogProvider::new();
        let schema_a = MemorySchemaProvider::new();
        schema_a
            .register_table("table_a".to_owned(), test::table_with_sequence(1, 1)?)?;
        catalog_a.register_schema("schema_a", Arc::new(schema_a))?;
        ctx.register_catalog("catalog_a", Arc::new(catalog_a));

        let catalog_b = MemoryCatalogProvider::new();
        let schema_b = MemorySchemaProvider::new();
        schema_b
            .register_table("table_b".to_owned(), test::table_with_sequence(1, 2)?)?;
        catalog_b.register_schema("schema_b", Arc::new(schema_b))?;
        ctx.register_catalog("catalog_b", Arc::new(catalog_b));

        let result = plan_and_collect(
            &ctx,
            "SELECT cat, SUM(i) AS total FROM (
                    SELECT i, 'a' AS cat FROM catalog_a.schema_a.table_a
                    UNION ALL
                    SELECT i, 'b' AS cat FROM catalog_b.schema_b.table_b
                ) AS all
                GROUP BY cat
                ORDER BY cat
                ",
        )
        .await?;

        let expected = vec![
            "+-----+-------+",
            "| cat | total |",
            "+-----+-------+",
            "| a   | 1     |",
            "| b   | 3     |",
            "+-----+-------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }

    #[tokio::test]
    async fn catalogs_not_leaked() {
        // the information schema used to introduce cyclic Arcs
        let ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        // register a single catalog
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let catalog_weak = Arc::downgrade(&catalog);
        ctx.register_catalog("my_catalog", catalog);

        let catalog_list_weak = {
            let state = ctx.state.read();
            Arc::downgrade(&state.catalog_list)
        };

        drop(ctx);

        assert_eq!(Weak::strong_count(&catalog_list_weak), 0);
        assert_eq!(Weak::strong_count(&catalog_weak), 0);
    }

    #[tokio::test]
    async fn sql_create_schema() -> Result<()> {
        // the information schema used to introduce cyclic Arcs
        let ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        // Create schema
        ctx.sql("CREATE SCHEMA abc").await?.collect().await?;

        // Add table to schema
        ctx.sql("CREATE TABLE abc.y AS VALUES (1,2,3)")
            .await?
            .collect()
            .await?;

        // Check table exists in schema
        let results = ctx.sql("SELECT * FROM information_schema.tables WHERE table_schema='abc' AND table_name = 'y'").await.unwrap().collect().await.unwrap();

        assert_eq!(results[0].num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn sql_create_catalog() -> Result<()> {
        // the information schema used to introduce cyclic Arcs
        let ctx = SessionContext::with_config(
            SessionConfig::new().with_information_schema(true),
        );

        // Create catalog
        ctx.sql("CREATE DATABASE test").await?.collect().await?;

        // Create schema
        ctx.sql("CREATE SCHEMA test.abc").await?.collect().await?;

        // Add table to schema
        ctx.sql("CREATE TABLE test.abc.y AS VALUES (1,2,3)")
            .await?
            .collect()
            .await?;

        // Check table exists in schema
        let results = ctx.sql("SELECT * FROM information_schema.tables WHERE table_catalog='test' AND table_schema='abc' AND table_name = 'y'").await.unwrap().collect().await.unwrap();

        assert_eq!(results[0].num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn read_with_glob_path() -> Result<()> {
        let ctx = SessionContext::new();

        let df = ctx
            .read_parquet(
                format!("{}/alltypes_plain*.parquet", parquet_test_data()),
                ParquetReadOptions::default(),
            )
            .await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
        // alltypes_plain.parquet = 8 rows, alltypes_plain.snappy.parquet = 2 rows, alltypes_dictionary.parquet = 2 rows
        assert_eq!(total_rows, 10);
        Ok(())
    }

    #[tokio::test]
    async fn read_with_glob_path_issue_2465() -> Result<()> {
        let ctx = SessionContext::new();

        let df = ctx
            .read_parquet(
                // it was reported that when a path contains // (two consecutive separator) no files were found
                // in this test, regardless of parquet_test_data() value, our path now contains a //
                format!("{}/..//*/alltypes_plain*.parquet", parquet_test_data()),
                ParquetReadOptions::default(),
            )
            .await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
        // alltypes_plain.parquet = 8 rows, alltypes_plain.snappy.parquet = 2 rows, alltypes_dictionary.parquet = 2 rows
        assert_eq!(total_rows, 10);
        Ok(())
    }

    #[tokio::test]
    async fn read_from_registered_table_with_glob_path() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_parquet(
            "test",
            &format!("{}/alltypes_plain*.parquet", parquet_test_data()),
            ParquetReadOptions::default(),
        )
        .await?;
        let df = ctx.sql("SELECT * FROM test").await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|rb| rb.num_rows()).sum();
        // alltypes_plain.parquet = 8 rows, alltypes_plain.snappy.parquet = 2 rows, alltypes_dictionary.parquet = 2 rows
        assert_eq!(total_rows, 10);
        Ok(())
    }

    struct MyPhysicalPlanner {}

    #[async_trait]
    impl PhysicalPlanner for MyPhysicalPlanner {
        async fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
            _session_state: &SessionState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Err(DataFusionError::NotImplemented(
                "query not supported".to_string(),
            ))
        }

        fn create_physical_expr(
            &self,
            _expr: &Expr,
            _input_dfschema: &crate::common::DFSchema,
            _input_schema: &Schema,
            _session_state: &SessionState,
        ) -> Result<Arc<dyn crate::physical_plan::PhysicalExpr>> {
            unimplemented!()
        }
    }

    struct MyQueryPlanner {}

    #[async_trait]
    impl QueryPlanner for MyQueryPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            session_state: &SessionState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let physical_planner = MyPhysicalPlanner {};
            physical_planner
                .create_physical_plan(logical_plan, session_state)
                .await
        }
    }

    /// Execute SQL and return results
    async fn plan_and_collect(
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        ctx.sql(sql).await?.collect().await
    }

    /// Generate CSV partitions within the supplied directory
    fn populate_csv_partitions(
        tmp_dir: &TempDir,
        partition_count: usize,
        file_extension: &str,
    ) -> Result<SchemaRef> {
        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
            Field::new("c3", DataType::Boolean, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{partition}.{file_extension}");
            let file_path = tmp_dir.path().join(filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
                file.write_all(data.as_bytes())?;
            }
        }

        Ok(schema)
    }

    /// Generate a partitioned CSV file and register it with an execution context
    async fn create_ctx(
        tmp_dir: &TempDir,
        partition_count: usize,
    ) -> Result<SessionContext> {
        let ctx =
            SessionContext::with_config(SessionConfig::new().with_target_partitions(8));

        let schema = populate_csv_partitions(tmp_dir, partition_count, ".csv")?;

        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        Ok(ctx)
    }

    // Test for compilation error when calling read_* functions from an #[async_trait] function.
    // See https://github.com/apache/arrow-datafusion/issues/1154
    #[async_trait]
    trait CallReadTrait {
        async fn call_read_csv(&self) -> DataFrame;
        async fn call_read_avro(&self) -> DataFrame;
        async fn call_read_parquet(&self) -> DataFrame;
    }

    struct CallRead {}

    #[async_trait]
    impl CallReadTrait for CallRead {
        async fn call_read_csv(&self) -> DataFrame {
            let ctx = SessionContext::new();
            ctx.read_csv("dummy", CsvReadOptions::new()).await.unwrap()
        }

        async fn call_read_avro(&self) -> DataFrame {
            let ctx = SessionContext::new();
            ctx.read_avro("dummy", AvroReadOptions::default())
                .await
                .unwrap()
        }

        async fn call_read_parquet(&self) -> DataFrame {
            let ctx = SessionContext::new();
            ctx.read_parquet("dummy", ParquetReadOptions::default())
                .await
                .unwrap()
        }
    }
}
