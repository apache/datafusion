// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`InformationSchemaProvider`] that implements the SQL [Information Schema] for DataFusion.
//!
//! [Information Schema]: https://en.wikipedia.org/wiki/Information_schema

use crate::streaming::StreamingTable;
use crate::table::TableFunction;
use crate::{CatalogProviderList, SchemaProvider, TableProvider};
use arrow::array::StringArray;
use arrow::array::builder::{ListBuilder, StringBuilder};
use arrow::compute::kernels::filter::filter_record_batch;
use arrow::compute::like;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::types::{LogicalType, NativeType};
use datafusion_common::{
    DataFusionError, ResolvedTableReference, plan_datafusion_err, plan_err,
};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_expr::function::WindowUDFFieldArgs;
use datafusion_expr::logical_plan::information_schema::{
    ColumnsDetail, InformationSchemaColumnsBuilder, InformationSchemaDfSettingsBuilder,
    InformationSchemaParametersBuilder, InformationSchemaRoutinesBuilder,
    InformationSchemaTablesBuilder, InformationSchemaViewBuilder, columns_schema,
    df_settings_schema, parameters_schema, routines_schema, show_functions_schema,
    tables_schema, views_schema,
};
pub use datafusion_expr::logical_plan::information_schema::{
    InformationSchemataBuilder, schemata_schema,
};
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{
    AggregateUDF, Documentation, ReturnFieldArgs, ScalarUDF, Signature, TypeSignature,
    WindowUDF,
};
use datafusion_expr::{TableType, Volatility};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion_session::CatalogProvider;
use itertools::Itertools;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

/// Name of the `information_schema` schema
pub const INFORMATION_SCHEMA: &str = "information_schema";
pub(crate) const TABLES: &str = "tables";
pub(crate) const VIEWS: &str = "views";
pub(crate) const COLUMNS: &str = "columns";
pub(crate) const DF_SETTINGS: &str = "df_settings";
pub(crate) const SCHEMATA: &str = "schemata";
pub(crate) const ROUTINES: &str = "routines";
pub(crate) const PARAMETERS: &str = "parameters";

/// All information schema tables
pub const INFORMATION_SCHEMA_TABLES: &[&str] = &[
    TABLES,
    VIEWS,
    COLUMNS,
    DF_SETTINGS,
    SCHEMATA,
    ROUTINES,
    PARAMETERS,
];

/// Implements the `information_schema` virtual schema and tables, and the
/// `SHOW` statements which are based on the same data.
///
/// The underlying tables in the `information_schema` are created on
/// demand. This means that if more tables are added to the underlying
/// providers, they will appear the next time the `information_schema`
/// table is queried.
#[derive(Debug)]
pub struct InformationSchemaProvider {
    config: InformationSchemaConfig,
}

impl InformationSchemaProvider {
    /// Creates a new [`InformationSchemaProvider`] for the provided `catalog_list`
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        Self {
            config: InformationSchemaConfig {
                information_schema_enabled: true,
                catalog_list,
                catalog_filter: None,
                table_functions: HashMap::new(),
            },
        }
    }

    /// Attach the session's table (UDTF) functions so that they appear in
    /// `information_schema.routines` / `SHOW FUNCTIONS`.
    pub fn with_table_functions(
        mut self,
        table_functions: HashMap<String, Arc<TableFunction>>,
    ) -> Self {
        self.config.table_functions = table_functions;
        self
    }

    /// Sets whether the `information_schema` is enabled. When disabled, its own
    /// tables are not reported and cannot be described.
    pub fn with_information_schema_enabled(mut self, enabled: bool) -> Self {
        self.config.information_schema_enabled = enabled;
        self
    }

    /// Restricts the reported objects to the given catalog. When `None`, all
    /// catalogs are reported.
    pub fn with_catalog_filter(mut self, catalog_name: Option<String>) -> Self {
        self.config.catalog_filter = catalog_name;
        self
    }

    /// Creates an execution plan for the `SHOW TABLES` statement.
    pub fn show_tables(&self) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingTableExec::try_new(
            SchemaRef::from(tables_schema()),
            vec![Arc::new(InformationSchemaTables::new(self.config.clone()))],
            None,
            vec![],
            false,
            None,
        )?))
    }

    /// Creates an execution plan for a `DESCRIBE <query>` statement, where
    /// `schema` is the output schema of the query.
    pub fn describe_schema(&self, schema: &Schema) -> Result<Arc<dyn ExecutionPlan>> {
        let mut builder = InformationSchemaColumnsBuilder::new(ColumnsDetail::Describe);
        for (position, field) in schema.fields().iter().enumerate() {
            builder.add_column("", "", "", position, field);
        }
        record_batch_to_exec(builder.finish())
    }

    /// Creates an execution plan for a `DESCRIBE <table>` or
    /// `SHOW [FULL|EXTENDED] COLUMNS FROM <table>` statement.
    pub async fn show_columns(
        &self,
        table_ref: &ResolvedTableReference,
        detail: ColumnsDetail,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.resolve_table(table_ref).await?;

        let mut builder = InformationSchemaColumnsBuilder::new(detail);
        for (position, field) in table.schema().fields().iter().enumerate() {
            builder.add_column(
                &table_ref.catalog,
                &table_ref.schema,
                &table_ref.table,
                position,
                field,
            );
        }
        record_batch_to_exec(builder.finish())
    }

    /// Creates an execution plan for a `SHOW CREATE TABLE <table>` statement.
    pub async fn show_create_table(
        &self,
        table_ref: &ResolvedTableReference,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table = self.resolve_table(table_ref).await?;

        let mut builder = InformationSchemaViewBuilder::new();
        builder.add_view(
            table_ref.catalog.as_ref(),
            table_ref.schema.as_ref(),
            table_ref.table.as_ref(),
            table.get_table_definition(),
        );
        let batch = builder.finish();
        record_batch_to_exec(batch)
    }

    async fn resolve_table(
        &self,
        table_ref: &ResolvedTableReference,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let not_found = || plan_datafusion_err!("table '{table_ref}' not found");

        // Look up the catalog before the `information_schema` special case so that
        // `information_schema` tables in a non-existent catalog are reported as not found.
        let catalog = self
            .config
            .catalog_list
            .catalog(&table_ref.catalog)
            .ok_or_else(not_found)?;

        let table = if table_ref.schema.as_ref() == INFORMATION_SCHEMA
            && self.config.information_schema_enabled
        {
            self.table(&table_ref.table).await?
        } else {
            catalog
                .schema(&table_ref.schema)
                .ok_or_else(not_found)?
                .table(&table_ref.table)
                .await?
        }
        .ok_or_else(not_found)?;
        Ok(table)
    }

    /// Creates an execution plan for a `SHOW <variable>` statement, or for
    /// `SHOW ALL` when `name` is `None`.
    pub fn show_variables(
        &self,
        config_options: &ConfigOptions,
        runtime_env: &Arc<RuntimeEnv>,
        name: Option<&str>,
        verbose: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let name = name.map(|n| {
            if n == "timezone" || n == "time.zone" {
                // we could introduce alias in OptionDefinition if this string matching thing grows
                "datafusion.execution.time_zone"
            } else {
                n
            }
        });

        if let Some(name) = name {
            // Report unknown variables as an error rather than an empty result
            let is_valid_variable =
                config_options.entries().iter().any(|opt| opt.key == name);
            let is_runtime_variable = name.starts_with("datafusion.runtime.");
            if !is_valid_variable && !is_runtime_variable {
                return plan_err!(
                    "'{name}' is not a variable which can be viewed with 'SHOW'"
                );
            }
        }
        let mut builder = InformationSchemaDfSettingsBuilder::new(verbose);
        self.config
            .make_df_settings(config_options, runtime_env, name, &mut builder);
        let batch = builder.finish();
        record_batch_to_exec(batch)
    }

    /// Creates an execution plan for a `SHOW FUNCTIONS [LIKE <pattern>]`
    /// statement. `filter` is the `LIKE` pattern.
    pub fn show_functions(
        &self,
        udfs: &HashMap<String, Arc<ScalarUDF>>,
        udafs: &HashMap<String, Arc<AggregateUDF>>,
        udwfs: &HashMap<String, Arc<WindowUDF>>,
        filter: Option<&str>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut builder = ShowFunctionsBuilder::new();

        for (name, udf) in udfs {
            for (arg_types, return_type) in get_udf_args_and_return_types(udf)? {
                builder.add_overload(
                    name,
                    &arg_types,
                    return_type.as_deref(),
                    udf.documentation(),
                    "SCALAR",
                );
            }
        }
        for (name, udaf) in udafs {
            for (arg_types, return_type) in get_udaf_args_and_return_types(udaf)? {
                builder.add_overload(
                    name,
                    &arg_types,
                    return_type.as_deref(),
                    udaf.documentation(),
                    "AGGREGATE",
                );
            }
        }
        for (name, udwf) in udwfs {
            for (arg_types, return_type) in get_udwf_args_and_return_types(udwf)? {
                builder.add_overload(
                    name,
                    &arg_types,
                    return_type.as_deref(),
                    udwf.documentation(),
                    "WINDOW",
                );
            }
        }
        // UDTFs have no signature or documentation
        for name in self.config.table_functions.keys() {
            builder.add_table_function(name);
        }

        record_batch_to_exec(builder.finish(filter)?)
    }
}

/// Wraps a [`RecordBatch`] in a single partition in-memory [`ExecutionPlan`].
fn record_batch_to_exec(record_batch: RecordBatch) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = record_batch.schema();
    let partitions = vec![vec![record_batch]];
    let mem_exec = MemorySourceConfig::try_new_exec(&partitions, schema, None)?;
    Ok(mem_exec)
}

/// Builds `SHOW FUNCTIONS` rows: one per signature of each scalar, aggregate
/// and window function, plus one per table function.
struct ShowFunctionsBuilder {
    function_names: StringBuilder,
    return_types: StringBuilder,
    parameters: ListBuilder<StringBuilder>,
    parameter_types: ListBuilder<StringBuilder>,
    function_types: StringBuilder,
    descriptions: StringBuilder,
    syntax_examples: StringBuilder,
}

impl ShowFunctionsBuilder {
    fn new() -> Self {
        Self {
            function_names: StringBuilder::new(),
            return_types: StringBuilder::new(),
            parameters: ListBuilder::new(StringBuilder::new()),
            parameter_types: ListBuilder::new(StringBuilder::new()),
            function_types: StringBuilder::new(),
            descriptions: StringBuilder::new(),
            syntax_examples: StringBuilder::new(),
        }
    }

    /// Append one row for a single function signature.
    fn add_overload(
        &mut self,
        name: &str,
        arg_types: &[String],
        return_type: Option<&str>,
        documentation: Option<&Documentation>,
        function_type: &str,
    ) {
        self.function_names.append_value(name);
        self.return_types.append_option(return_type);

        // Documented argument names are matched to the signature by position
        let arg_names = documentation.and_then(|d| d.arguments.as_ref());
        for (position, type_name) in arg_types.iter().enumerate() {
            let param_name = arg_names
                .and_then(|args| args.get(position))
                .map(|(arg_name, _)| arg_name.as_str());
            self.parameters.values().append_option(param_name);
            self.parameter_types.values().append_value(type_name);
        }
        self.parameters.append(true);
        self.parameter_types.append(true);

        self.function_types.append_value(function_type);
        self.descriptions
            .append_option(documentation.map(|d| d.description.as_str()));
        self.syntax_examples
            .append_option(documentation.map(|d| d.syntax_example.as_str()));
    }

    /// Append one row for a table function.
    fn add_table_function(&mut self, name: &str) {
        self.function_names.append_value(name);
        self.return_types.append_value("TABLE");
        self.parameters.append(false);
        self.parameter_types.append(false);
        self.function_types.append_value("TABLE");
        self.descriptions.append_null();
        self.syntax_examples.append_null();
    }

    /// Finalize into a [`RecordBatch`], keeping only the functions whose name
    /// matches the `LIKE` pattern `filter`.
    fn finish(mut self, filter: Option<&str>) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            SchemaRef::from(show_functions_schema()),
            vec![
                Arc::new(self.function_names.finish()),
                Arc::new(self.return_types.finish()),
                Arc::new(self.parameters.finish()),
                Arc::new(self.parameter_types.finish()),
                Arc::new(self.function_types.finish()),
                Arc::new(self.descriptions.finish()),
                Arc::new(self.syntax_examples.finish()),
            ],
        )?;
        match filter {
            Some(pattern) => {
                let mask = like(batch.column(0), &StringArray::new_scalar(pattern))?;
                Ok(filter_record_batch(&batch, &mask)?)
            }
            None => Ok(batch),
        }
    }
}

#[derive(Clone, Debug)]
struct InformationSchemaConfig {
    information_schema_enabled: bool,
    catalog_list: Arc<dyn CatalogProviderList>,
    catalog_filter: Option<String>,
    table_functions: HashMap<String, Arc<TableFunction>>,
}

impl InformationSchemaConfig {
    /// The catalogs to report: only `catalog_filter` if set, otherwise all.
    fn catalogs(&self) -> Result<Vec<(String, Arc<dyn CatalogProvider>)>> {
        match &self.catalog_filter {
            Some(catalog_name) => {
                let catalog =
                    self.catalog_list.catalog(catalog_name).ok_or_else(|| {
                        plan_datafusion_err!("catalog '{catalog_name}' not found")
                    })?;
                Ok(vec![(catalog_name.clone(), catalog)])
            }
            // Skip catalogs that were deregistered after `catalog_names` was called
            None => Ok(self
                .catalog_list
                .catalog_names()
                .into_iter()
                .filter_map(|name| {
                    let catalog = self.catalog_list.catalog(&name)?;
                    Some((name, catalog))
                })
                .collect()),
        }
    }

    /// Construct the `information_schema.tables` virtual table
    async fn make_tables(
        &self,
        builder: &mut InformationSchemaTablesBuilder,
    ) -> Result<(), DataFusionError> {
        for (catalog_name, catalog) in self.catalogs()? {
            self.add_tables(builder, &catalog_name, catalog.as_ref())
                .await?;
        }
        Ok(())
    }

    async fn add_tables(
        &self,
        builder: &mut InformationSchemaTablesBuilder,
        catalog_name: &str,
        catalog: &dyn CatalogProvider,
    ) -> Result<(), DataFusionError> {
        for schema_name in catalog.schema_names() {
            if schema_name != INFORMATION_SCHEMA {
                // schema name may not exist in the catalog, so we need to check
                if let Some(schema) = catalog.schema(&schema_name) {
                    for table_name in schema.table_names() {
                        if let Some(table_type) = schema.table_type(&table_name).await? {
                            builder.add_table(
                                catalog_name,
                                &schema_name,
                                &table_name,
                                table_type,
                            );
                        }
                    }
                }
            }
        }
        if self.information_schema_enabled {
            // Add a final list for the information schema tables themselves
            for table_name in INFORMATION_SCHEMA_TABLES {
                builder.add_table(
                    catalog_name,
                    INFORMATION_SCHEMA,
                    table_name,
                    TableType::View,
                );
            }
        }
        Ok(())
    }

    fn make_schemata(
        &self,
        builder: &mut InformationSchemataBuilder,
    ) -> Result<(), DataFusionError> {
        for (catalog_name, catalog) in self.catalogs()? {
            Self::add_schemata(builder, &catalog_name, catalog.as_ref());
        }
        Ok(())
    }

    fn add_schemata(
        builder: &mut InformationSchemataBuilder,
        catalog_name: &str,
        catalog: &dyn CatalogProvider,
    ) {
        for schema_name in catalog.schema_names() {
            if schema_name != INFORMATION_SCHEMA
                && let Some(schema) = catalog.schema(&schema_name)
            {
                let schema_owner = schema.owner_name();
                builder.add_schemata(catalog_name, &schema_name, schema_owner);
            }
        }
    }

    async fn make_views(
        &self,
        builder: &mut InformationSchemaViewBuilder,
    ) -> Result<(), DataFusionError> {
        for (catalog_name, catalog) in self.catalogs()? {
            Self::add_views(builder, &catalog_name, catalog.as_ref()).await?;
        }
        Ok(())
    }

    async fn add_views(
        builder: &mut InformationSchemaViewBuilder,
        catalog_name: &str,
        catalog: &dyn CatalogProvider,
    ) -> Result<(), DataFusionError> {
        for schema_name in catalog.schema_names() {
            if schema_name != INFORMATION_SCHEMA {
                // schema name may not exist in the catalog, so we need to check
                if let Some(schema) = catalog.schema(&schema_name) {
                    for table_name in schema.table_names() {
                        if let Some(table) = schema.table(&table_name).await? {
                            builder.add_view(
                                catalog_name,
                                &schema_name,
                                &table_name,
                                table.get_table_definition(),
                            )
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Construct the `information_schema.columns` virtual table
    async fn make_columns(
        &self,
        builder: &mut InformationSchemaColumnsBuilder,
    ) -> Result<(), DataFusionError> {
        for (catalog_name, catalog) in self.catalogs()? {
            Self::add_columns(builder, &catalog_name, catalog.as_ref()).await?;
        }
        Ok(())
    }

    async fn add_columns(
        builder: &mut InformationSchemaColumnsBuilder,
        catalog_name: &str,
        catalog: &dyn CatalogProvider,
    ) -> Result<(), DataFusionError> {
        for schema_name in catalog.schema_names() {
            if schema_name != INFORMATION_SCHEMA {
                // schema name may not exist in the catalog, so we need to check
                if let Some(schema) = catalog.schema(&schema_name) {
                    for table_name in schema.table_names() {
                        if let Some(table) = schema.table(&table_name).await? {
                            for (field_position, field) in
                                table.schema().fields().iter().enumerate()
                            {
                                builder.add_column(
                                    catalog_name,
                                    &schema_name,
                                    &table_name,
                                    field_position,
                                    field,
                                )
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Construct the `information_schema.df_settings` virtual table, restricted
    /// to the setting `name` if given. All settings are ordered by name.
    fn make_df_settings(
        &self,
        config_options: &ConfigOptions,
        runtime_env: &Arc<RuntimeEnv>,
        name: Option<&str>,
        builder: &mut InformationSchemaDfSettingsBuilder,
    ) {
        let mut entries = config_options.entries();
        entries.extend(runtime_env.config_entries());

        match name {
            Some(name) => entries.retain(|entry| entry.key == name),
            None => entries.sort_unstable_by(|a, b| a.key.cmp(&b.key)),
        }

        for entry in entries {
            builder.add_setting(entry);
        }
    }

    fn make_routines(
        &self,
        udfs: &HashMap<String, Arc<ScalarUDF>>,
        udafs: &HashMap<String, Arc<AggregateUDF>>,
        udwfs: &HashMap<String, Arc<WindowUDF>>,
        config_options: &ConfigOptions,
        builder: &mut InformationSchemaRoutinesBuilder,
    ) -> Result<()> {
        let catalog_name = &config_options.catalog.default_catalog;
        let schema_name = &config_options.catalog.default_schema;

        for (name, udf) in udfs {
            let return_types = get_udf_args_and_return_types(udf)?
                .into_iter()
                .map(|(_, return_type)| return_type)
                .collect::<HashSet<_>>();
            for return_type in return_types {
                builder.add_routine(
                    catalog_name,
                    schema_name,
                    name,
                    "FUNCTION",
                    Self::is_deterministic(udf.signature()),
                    return_type.as_ref(),
                    "SCALAR",
                    udf.documentation().map(|d| d.description.as_str()),
                    udf.documentation().map(|d| d.syntax_example.as_str()),
                )
            }
        }

        for (name, udaf) in udafs {
            let return_types = get_udaf_args_and_return_types(udaf)?
                .into_iter()
                .map(|(_, return_type)| return_type)
                .collect::<HashSet<_>>();
            for return_type in return_types {
                builder.add_routine(
                    catalog_name,
                    schema_name,
                    name,
                    "FUNCTION",
                    Self::is_deterministic(udaf.signature()),
                    return_type.as_ref(),
                    "AGGREGATE",
                    udaf.documentation().map(|d| d.description.as_str()),
                    udaf.documentation().map(|d| d.syntax_example.as_str()),
                )
            }
        }

        for (name, udwf) in udwfs {
            let return_types = get_udwf_args_and_return_types(udwf)?
                .into_iter()
                .map(|(_, return_type)| return_type)
                .collect::<HashSet<_>>();
            for return_type in return_types {
                builder.add_routine(
                    catalog_name,
                    schema_name,
                    name,
                    "FUNCTION",
                    Self::is_deterministic(udwf.signature()),
                    return_type.as_ref(),
                    "WINDOW",
                    udwf.documentation().map(|d| d.description.as_str()),
                    udwf.documentation().map(|d| d.syntax_example.as_str()),
                )
            }
        }

        // UDTFs have no signature; report a single `TABLE`-typed row for each
        for name in self.table_functions.keys() {
            builder.add_routine(
                catalog_name,
                schema_name,
                name,
                "FUNCTION",
                // No signature is available for UDTFs; report deterministic
                // = false to stay conservative.
                false,
                Some(&"TABLE"),
                "TABLE",
                None::<String>,
                None::<String>,
            )
        }
        Ok(())
    }

    fn is_deterministic(signature: &Signature) -> bool {
        signature.volatility == Volatility::Immutable
    }
    fn make_parameters(
        &self,
        udfs: &HashMap<String, Arc<ScalarUDF>>,
        udafs: &HashMap<String, Arc<AggregateUDF>>,
        udwfs: &HashMap<String, Arc<WindowUDF>>,
        config_options: &ConfigOptions,
        builder: &mut InformationSchemaParametersBuilder,
    ) -> Result<()> {
        let catalog_name = &config_options.catalog.default_catalog;
        let schema_name = &config_options.catalog.default_schema;
        let mut add_parameters = |func_name: &str,
                                  args: Option<&Vec<(String, String)>>,
                                  arg_types: Vec<String>,
                                  return_type: Option<String>,
                                  is_variadic: bool,
                                  rid: u8| {
            for (position, type_name) in arg_types.iter().enumerate() {
                let param_name =
                    args.and_then(|a| a.get(position).map(|arg| arg.0.as_str()));
                builder.add_parameter(
                    catalog_name,
                    schema_name,
                    func_name,
                    position as u64 + 1,
                    "IN",
                    param_name,
                    type_name,
                    None::<&str>,
                    is_variadic,
                    rid,
                );
            }
            if let Some(return_type) = return_type {
                builder.add_parameter(
                    catalog_name,
                    schema_name,
                    func_name,
                    1,
                    "OUT",
                    None::<&str>,
                    return_type.as_str(),
                    None::<&str>,
                    false,
                    rid,
                );
            }
        };

        for (func_name, udf) in udfs {
            let args = udf.documentation().and_then(|d| d.arguments.as_ref());
            let combinations = get_udf_args_and_return_types(udf)?;
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args,
                    arg_types,
                    return_type,
                    Self::is_variadic(udf.signature()),
                    rid as u8,
                );
            }
        }

        for (func_name, udaf) in udafs {
            let args = udaf.documentation().and_then(|d| d.arguments.as_ref());
            let combinations = get_udaf_args_and_return_types(udaf)?;
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args,
                    arg_types,
                    return_type,
                    Self::is_variadic(udaf.signature()),
                    rid as u8,
                );
            }
        }

        for (func_name, udwf) in udwfs {
            let args = udwf.documentation().and_then(|d| d.arguments.as_ref());
            let combinations = get_udwf_args_and_return_types(udwf)?;
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args,
                    arg_types,
                    return_type,
                    Self::is_variadic(udwf.signature()),
                    rid as u8,
                );
            }
        }

        // UDTFs are deliberately omitted: a scalar UDF with the same name (e.g.
        // `generate_series`) would be indistinguishable from it here.

        Ok(())
    }

    fn is_variadic(signature: &Signature) -> bool {
        matches!(
            signature.type_signature,
            TypeSignature::Variadic(_) | TypeSignature::VariadicAny
        )
    }
}

/// Origins used to enumerate the physical types a native type can take
const RESOLVE_CAST_SOURCES: [DataType; 2] = [DataType::Null, DataType::LargeUtf8];

/// Build argument fields for `information_schema` to provide possible return types
fn resolve_informational_fields(idx: usize, t: &NativeType) -> Vec<FieldRef> {
    // Since native types map to several physical types, resolve it against
    // ambiguous types to get canonical `DataType`s for the native type.
    // Skip origins the type has no cast from (e.g. `Struct` from `LargeUtf8`)
    RESOLVE_CAST_SOURCES
        .iter()
        .filter_map(|source| t.default_cast_for(source).ok())
        .unique()
        .map(|dt| Arc::new(Field::new(format!("arg_{idx}"), dt, true)))
        .collect()
}

/// Function information schema is a set of tuples - argument types and an optional return type
type FunctionInformationSchema = BTreeSet<(Vec<String>, Option<String>)>;

/// Get the arguments and return types of a function from its signature
fn get_args_and_return_types(
    signature: &Signature,
    return_field: impl Fn(&[FieldRef]) -> Result<FieldRef>,
) -> Result<FunctionInformationSchema> {
    let arg_types = signature.type_signature.get_representative_types();
    if arg_types.is_empty() {
        // Edge case if function doesn't have arguments
        return Ok(BTreeSet::from([(vec![], None)]));
    }
    arg_types
        .into_iter()
        .map(|arg_types| {
            // Get possible types for each input arg
            let arg_fields = arg_types
                .iter()
                .enumerate()
                .map(|(i, t)| resolve_informational_fields(i, t))
                .collect::<Vec<_>>();
            // Build combinations of arg types with the return type
            let return_types = arg_fields
                .into_iter()
                .multi_cartesian_product()
                .filter_map(|arg_fields| {
                    return_field(&arg_fields)
                        .ok()
                        .map(|f| Some(remove_native_type_prefix(&f.data_type().into())))
                })
                .collect::<BTreeSet<_>>();
            let return_types = if return_types.is_empty() {
                // Indicate `None` if the return type cannot be represented from a signature,
                BTreeSet::from([None])
            } else {
                return_types
            };
            let arg_types = arg_types
                .iter()
                .map(remove_native_type_prefix)
                .collect::<Vec<_>>();
            let tuples = return_types
                .into_iter()
                .map(move |return_type| (arg_types.clone(), return_type));
            Ok(tuples)
        })
        .flatten_ok()
        .collect()
}

fn get_udf_args_and_return_types(
    udf: &Arc<ScalarUDF>,
) -> Result<FunctionInformationSchema> {
    get_args_and_return_types(udf.signature(), |arg_fields| {
        let arg_fields = &fields_with_udf(arg_fields, udf.as_ref())?;
        let scalar_arguments = &vec![None; arg_fields.len()];
        udf.return_field_from_args(ReturnFieldArgs {
            arg_fields,
            scalar_arguments,
        })
    })
}

fn get_udaf_args_and_return_types(
    udaf: &Arc<AggregateUDF>,
) -> Result<FunctionInformationSchema> {
    get_args_and_return_types(udaf.signature(), |arg_fields| {
        let arg_fields = &fields_with_udf(arg_fields, udaf.as_ref())?;
        udaf.return_field(arg_fields)
    })
}

fn get_udwf_args_and_return_types(
    udwf: &Arc<WindowUDF>,
) -> Result<FunctionInformationSchema> {
    get_args_and_return_types(udwf.signature(), |arg_fields| {
        let arg_fields = &fields_with_udf(arg_fields, udwf.as_ref())?;
        udwf.field(WindowUDFFieldArgs::new(arg_fields, udwf.name()))
    })
}

#[inline]
fn remove_native_type_prefix(native_type: &NativeType) -> String {
    format!("{native_type}")
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        INFORMATION_SCHEMA_TABLES
            .iter()
            .map(|t| (*t).to_string())
            .collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.config.get_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        INFORMATION_SCHEMA_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}

impl InformationSchemaConfig {
    fn get_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let config = self.clone();
        let table: Arc<dyn PartitionStream> = match name.to_ascii_lowercase().as_str() {
            TABLES => Arc::new(InformationSchemaTables::new(config)),
            COLUMNS => Arc::new(InformationSchemaColumns::new(config)),
            VIEWS => Arc::new(InformationSchemaViews::new(config)),
            DF_SETTINGS => Arc::new(InformationSchemaDfSettings::new(config)),
            SCHEMATA => Arc::new(InformationSchemata::new(config)),
            ROUTINES => Arc::new(InformationSchemaRoutines::new(config)),
            PARAMETERS => Arc::new(InformationSchemaParameters::new(config)),
            _ => return Ok(None),
        };

        Ok(Some(Arc::new(
            StreamingTable::try_new(Arc::clone(table.schema()), vec![table]).unwrap(),
        )))
    }
}

#[derive(Debug)]
struct InformationSchemaTables {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaTables {
    fn new(config: InformationSchemaConfig) -> Self {
        Self {
            schema: Arc::new(tables_schema()),
            config,
        }
    }
}

impl PartitionStream for InformationSchemaTables {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = InformationSchemaTablesBuilder::new();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_tables(&mut builder).await?;
                Ok(builder.finish())
            }),
        ))
    }
}

#[derive(Debug)]
struct InformationSchemaViews {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaViews {
    fn new(config: InformationSchemaConfig) -> Self {
        Self {
            schema: Arc::new(views_schema()),
            config,
        }
    }
}

impl PartitionStream for InformationSchemaViews {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = InformationSchemaViewBuilder::new();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_views(&mut builder).await?;
                Ok(builder.finish())
            }),
        ))
    }
}

#[derive(Debug)]
struct InformationSchemaColumns {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaColumns {
    fn new(config: InformationSchemaConfig) -> Self {
        Self {
            schema: Arc::new(columns_schema(ColumnsDetail::Full)),
            config,
        }
    }
}

impl PartitionStream for InformationSchemaColumns {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = InformationSchemaColumnsBuilder::new(ColumnsDetail::Full);
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_columns(&mut builder).await?;
                Ok(builder.finish())
            }),
        ))
    }
}

#[derive(Debug)]
struct InformationSchemata {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemata {
    fn new(config: InformationSchemaConfig) -> Self {
        Self {
            schema: Arc::new(schemata_schema()),
            config,
        }
    }
}

impl PartitionStream for InformationSchemata {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = InformationSchemataBuilder::new();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_schemata(&mut builder)?;
                builder.finish()
            }),
        ))
    }
}

#[derive(Debug)]
struct InformationSchemaDfSettings {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaDfSettings {
    fn new(config: InformationSchemaConfig) -> Self {
        Self {
            schema: Arc::new(df_settings_schema(true)),
            config,
        }
    }
}

impl PartitionStream for InformationSchemaDfSettings {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = self.config.clone();
        let mut builder = InformationSchemaDfSettingsBuilder::new(true);
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                // create a mem table with the names of tables
                let runtime_env = ctx.runtime_env();
                config.make_df_settings(
                    ctx.session_config().options(),
                    &runtime_env,
                    None,
                    &mut builder,
                );
                Ok(builder.finish())
            }),
        ))
    }
}

#[derive(Debug)]
struct InformationSchemaRoutines {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaRoutines {
    fn new(config: InformationSchemaConfig) -> Self {
        Self {
            schema: Arc::new(routines_schema()),
            config,
        }
    }
}

impl PartitionStream for InformationSchemaRoutines {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = self.config.clone();
        let mut builder = InformationSchemaRoutinesBuilder::new();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move {
                config.make_routines(
                    ctx.scalar_functions(),
                    ctx.aggregate_functions(),
                    ctx.window_functions(),
                    ctx.session_config().options(),
                    &mut builder,
                )?;
                Ok(builder.finish())
            }),
        ))
    }
}

#[derive(Debug)]
struct InformationSchemaParameters {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaParameters {
    fn new(config: InformationSchemaConfig) -> Self {
        Self {
            schema: Arc::new(parameters_schema()),
            config,
        }
    }
}

impl PartitionStream for InformationSchemaParameters {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = self.config.clone();
        let mut builder = InformationSchemaParametersBuilder::new();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(async move {
                config.make_parameters(
                    ctx.scalar_functions(),
                    ctx.aggregate_functions(),
                    ctx.window_functions(),
                    ctx.session_config().options(),
                    &mut builder,
                )?;
                Ok(builder.finish())
            }),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CatalogProvider;
    use arrow::array::Array;
    use arrow::datatypes::Fields;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    #[tokio::test]
    async fn make_tables_uses_table_type() {
        let config = InformationSchemaConfig {
            information_schema_enabled: true,
            catalog_list: Arc::new(Fixture),
            catalog_filter: None,
            table_functions: HashMap::new(),
        };
        let mut builder = InformationSchemaTablesBuilder::new();

        assert!(config.make_tables(&mut builder).await.is_ok());

        let batch = builder.finish();
        let table_types = batch
            .column_by_name("table_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!("BASE TABLE", table_types.value(0));
    }

    // UDF
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestScalarUDF {
        signature: Signature,
    }
    impl ScalarUDFImpl for TestScalarUDF {
        fn name(&self) -> &str {
            "TestScalarUDF"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            Ok(arg_types.last().unwrap().clone())
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::from("a")))
        }
    }

    #[test]
    fn test_get_udf_args_and_return_types() -> Result<()> {
        // heterogeneous arguments to test mixed arguments retrieval
        let signature = Signature::exact(
            [
                vec![DataType::Int32; 6],
                vec![DataType::Float32; 6],
                vec![DataType::Utf8; 1],
            ]
            .concat(),
            Volatility::Stable,
        );
        let udf = Arc::new(ScalarUDF::from(TestScalarUDF { signature }));
        let result = get_udf_args_and_return_types(&udf)?;
        assert_eq!(result.len(), 1);
        let (args, ret) = result.iter().next().unwrap();
        assert_eq!(
            *args,
            [
                vec![String::from("Int32"); 6],
                vec![String::from("Float32"); 6],
                vec![String::from("String"); 1]
            ]
            .concat()
        );
        assert_eq!(*ret, Some(String::from("String")));

        Ok(())
    }

    #[test]
    fn test_get_udf_args_and_return_types_nested() -> Result<()> {
        let struct_type =
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]));
        let signature = Signature::exact(vec![struct_type], Volatility::Stable);
        let udf = Arc::new(ScalarUDF::from(TestScalarUDF { signature }));
        let result = get_udf_args_and_return_types(&udf)?;
        assert_eq!(result.len(), 1);
        Ok(())
    }

    #[derive(Debug)]
    struct Fixture;

    #[async_trait]
    impl SchemaProvider for Fixture {
        // InformationSchemaConfig::make_tables should use this.
        async fn table_type(&self, _: &str) -> Result<Option<TableType>> {
            Ok(Some(TableType::Base))
        }

        // InformationSchemaConfig::make_tables used this before `table_type`
        // existed but should not, as it may be expensive.
        async fn table(&self, _: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            panic!(
                "InformationSchemaConfig::make_tables called SchemaProvider::table instead of table_type"
            )
        }

        fn table_names(&self) -> Vec<String> {
            vec!["atable".to_string()]
        }

        fn table_exist(&self, _: &str) -> bool {
            unimplemented!("not required for these tests")
        }
    }

    impl CatalogProviderList for Fixture {
        fn register_catalog(
            &self,
            _: String,
            _: Arc<dyn CatalogProvider>,
        ) -> Option<Arc<dyn CatalogProvider>> {
            unimplemented!("not required for these tests")
        }

        fn catalog_names(&self) -> Vec<String> {
            vec!["acatalog".to_string()]
        }

        fn catalog(&self, _: &str) -> Option<Arc<dyn CatalogProvider>> {
            Some(Arc::new(Self))
        }
    }

    impl CatalogProvider for Fixture {
        fn schema_names(&self) -> Vec<String> {
            vec!["aschema".to_string()]
        }

        fn schema(&self, _: &str) -> Option<Arc<dyn SchemaProvider>> {
            Some(Arc::new(Self))
        }
    }
}
