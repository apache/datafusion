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

//! [`InformationSchemaProvider`] that implements the SQL [Information Schema] for DataFusion.
//!
//! [Information Schema]: https://en.wikipedia.org/wiki/Information_schema

use crate::streaming::StreamingTable;
use crate::{CatalogProviderList, SchemaProvider, TableProvider};
use arrow::array::builder::{BooleanBuilder, UInt8Builder};
use arrow::{
    array::{StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion_common::config::{ConfigEntry, ConfigOptions};
use datafusion_common::error::Result;
use datafusion_common::types::NativeType;
use datafusion_common::DataFusionError;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::{AggregateUDF, ScalarUDF, Signature, TypeSignature, WindowUDF};
use datafusion_expr::{TableType, Volatility};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use datafusion_physical_plan::SendableRecordBatchStream;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Debug;
use std::{any::Any, sync::Arc};

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

/// Implements the `information_schema` virtual schema and tables
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
            config: InformationSchemaConfig { catalog_list },
        }
    }
}

#[derive(Clone, Debug)]
struct InformationSchemaConfig {
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl InformationSchemaConfig {
    /// Construct the `information_schema.tables` virtual table
    async fn make_tables(
        &self,
        builder: &mut InformationSchemaTablesBuilder,
    ) -> Result<(), DataFusionError> {
        // create a mem table with the names of tables

        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    // schema name may not exist in the catalog, so we need to check
                    if let Some(schema) = catalog.schema(&schema_name) {
                        for table_name in schema.table_names() {
                            if let Some(table_type) =
                                schema.table_type(&table_name).await?
                            {
                                builder.add_table(
                                    &catalog_name,
                                    &schema_name,
                                    &table_name,
                                    table_type,
                                );
                            }
                        }
                    }
                }
            }

            // Add a final list for the information schema tables themselves
            for table_name in INFORMATION_SCHEMA_TABLES {
                builder.add_table(
                    &catalog_name,
                    INFORMATION_SCHEMA,
                    table_name,
                    TableType::View,
                );
            }
        }

        Ok(())
    }

    async fn make_schemata(&self, builder: &mut InformationSchemataBuilder) {
        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        let schema_owner = schema.owner_name();
                        builder.add_schemata(&catalog_name, &schema_name, schema_owner);
                    }
                }
            }
        }
    }

    async fn make_views(
        &self,
        builder: &mut InformationSchemaViewBuilder,
    ) -> Result<(), DataFusionError> {
        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    // schema name may not exist in the catalog, so we need to check
                    if let Some(schema) = catalog.schema(&schema_name) {
                        for table_name in schema.table_names() {
                            if let Some(table) = schema.table(&table_name).await? {
                                builder.add_view(
                                    &catalog_name,
                                    &schema_name,
                                    &table_name,
                                    table.get_table_definition(),
                                )
                            }
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
        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

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
                                        &catalog_name,
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
        }

        Ok(())
    }

    /// Construct the `information_schema.df_settings` virtual table
    fn make_df_settings(
        &self,
        config_options: &ConfigOptions,
        runtime_env: &Arc<RuntimeEnv>,
        builder: &mut InformationSchemaDfSettingsBuilder,
    ) {
        for entry in config_options.entries() {
            builder.add_setting(entry);
        }
        // Add runtime configuration entries
        for entry in runtime_env.config_entries() {
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
                    udf.documentation().map(|d| d.description.to_string()),
                    udf.documentation().map(|d| d.syntax_example.to_string()),
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
                    udaf.documentation().map(|d| d.description.to_string()),
                    udaf.documentation().map(|d| d.syntax_example.to_string()),
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
                    udwf.documentation().map(|d| d.description.to_string()),
                    udwf.documentation().map(|d| d.syntax_example.to_string()),
                )
            }
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
            let args = udf.documentation().and_then(|d| d.arguments.clone());
            let combinations = get_udf_args_and_return_types(udf)?;
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args.as_ref(),
                    arg_types,
                    return_type,
                    Self::is_variadic(udf.signature()),
                    rid as u8,
                );
            }
        }

        for (func_name, udaf) in udafs {
            let args = udaf.documentation().and_then(|d| d.arguments.clone());
            let combinations = get_udaf_args_and_return_types(udaf)?;
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args.as_ref(),
                    arg_types,
                    return_type,
                    Self::is_variadic(udaf.signature()),
                    rid as u8,
                );
            }
        }

        for (func_name, udwf) in udwfs {
            let args = udwf.documentation().and_then(|d| d.arguments.clone());
            let combinations = get_udwf_args_and_return_types(udwf)?;
            for (rid, (arg_types, return_type)) in combinations.into_iter().enumerate() {
                add_parameters(
                    func_name,
                    args.as_ref(),
                    arg_types,
                    return_type,
                    Self::is_variadic(udwf.signature()),
                    rid as u8,
                );
            }
        }

        Ok(())
    }

    fn is_variadic(signature: &Signature) -> bool {
        matches!(
            signature.type_signature,
            TypeSignature::Variadic(_) | TypeSignature::VariadicAny
        )
    }
}

/// get the arguments and return types of a UDF
/// returns a tuple of (arg_types, return_type)
fn get_udf_args_and_return_types(
    udf: &Arc<ScalarUDF>,
) -> Result<BTreeSet<(Vec<String>, Option<String>)>> {
    let signature = udf.signature();
    let arg_types = signature.type_signature.get_example_types();
    if arg_types.is_empty() {
        Ok(vec![(vec![], None)].into_iter().collect::<BTreeSet<_>>())
    } else {
        Ok(arg_types
            .into_iter()
            .map(|arg_types| {
                // only handle the function which implemented [`ScalarUDFImpl::return_type`] method
                let return_type = udf
                    .return_type(&arg_types)
                    .map(|t| remove_native_type_prefix(&NativeType::from(t)))
                    .ok();
                let arg_types = arg_types
                    .into_iter()
                    .map(|t| remove_native_type_prefix(&NativeType::from(t)))
                    .collect::<Vec<_>>();
                (arg_types, return_type)
            })
            .collect::<BTreeSet<_>>())
    }
}

fn get_udaf_args_and_return_types(
    udaf: &Arc<AggregateUDF>,
) -> Result<BTreeSet<(Vec<String>, Option<String>)>> {
    let signature = udaf.signature();
    let arg_types = signature.type_signature.get_example_types();
    if arg_types.is_empty() {
        Ok(vec![(vec![], None)].into_iter().collect::<BTreeSet<_>>())
    } else {
        Ok(arg_types
            .into_iter()
            .map(|arg_types| {
                // only handle the function which implemented [`ScalarUDFImpl::return_type`] method
                let return_type = udaf
                    .return_type(&arg_types)
                    .ok()
                    .map(|t| remove_native_type_prefix(&NativeType::from(t)));
                let arg_types = arg_types
                    .into_iter()
                    .map(|t| remove_native_type_prefix(&NativeType::from(t)))
                    .collect::<Vec<_>>();
                (arg_types, return_type)
            })
            .collect::<BTreeSet<_>>())
    }
}

fn get_udwf_args_and_return_types(
    udwf: &Arc<WindowUDF>,
) -> Result<BTreeSet<(Vec<String>, Option<String>)>> {
    let signature = udwf.signature();
    let arg_types = signature.type_signature.get_example_types();
    if arg_types.is_empty() {
        Ok(vec![(vec![], None)].into_iter().collect::<BTreeSet<_>>())
    } else {
        Ok(arg_types
            .into_iter()
            .map(|arg_types| {
                // only handle the function which implemented [`ScalarUDFImpl::return_type`] method
                let arg_types = arg_types
                    .into_iter()
                    .map(|t| remove_native_type_prefix(&NativeType::from(t)))
                    .collect::<Vec<_>>();
                (arg_types, None)
            })
            .collect::<BTreeSet<_>>())
    }
}

#[inline]
fn remove_native_type_prefix(native_type: &NativeType) -> String {
    format!("{native_type}")
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        let config = self.config.clone();
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

    fn table_exist(&self, name: &str) -> bool {
        INFORMATION_SCHEMA_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}

#[derive(Debug)]
struct InformationSchemaTables {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaTables {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaTablesBuilder {
        InformationSchemaTablesBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            table_types: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaTables {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
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

/// Builds the `information_schema.TABLE` table row by row
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-columns.html>
struct InformationSchemaTablesBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
}

impl InformationSchemaTablesBuilder {
    fn add_table(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        table_type: TableType,
    ) {
        // Note: append_value is actually infallible.
        self.catalog_names.append_value(catalog_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.table_types.append_value(match table_type {
            TableType::Base => "BASE TABLE",
            TableType::View => "VIEW",
            TableType::Temporary => "LOCAL TEMPORARY",
        });
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.table_types.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Debug)]
struct InformationSchemaViews {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaViews {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("definition", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaViewBuilder {
        InformationSchemaViewBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            definitions: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaViews {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
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

/// Builds the `information_schema.VIEWS` table row by row
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-columns.html>
struct InformationSchemaViewBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    definitions: StringBuilder,
}

impl InformationSchemaViewBuilder {
    fn add_view(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        definition: Option<&(impl AsRef<str> + ?Sized)>,
    ) {
        // Note: append_value is actually infallible.
        self.catalog_names.append_value(catalog_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.definitions.append_option(definition.as_ref());
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.definitions.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Debug)]
struct InformationSchemaColumns {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaColumns {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::UInt64, false),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("character_maximum_length", DataType::UInt64, true),
            Field::new("character_octet_length", DataType::UInt64, true),
            Field::new("numeric_precision", DataType::UInt64, true),
            Field::new("numeric_precision_radix", DataType::UInt64, true),
            Field::new("numeric_scale", DataType::UInt64, true),
            Field::new("datetime_precision", DataType::UInt64, true),
            Field::new("interval_type", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaColumnsBuilder {
        // StringBuilder requires providing an initial capacity, so
        // pick 10 here arbitrarily as this is not performance
        // critical code and the number of tables is unavailable here.
        let default_capacity = 10;

        InformationSchemaColumnsBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            column_names: StringBuilder::new(),
            ordinal_positions: UInt64Builder::with_capacity(default_capacity),
            column_defaults: StringBuilder::new(),
            is_nullables: StringBuilder::new(),
            data_types: StringBuilder::new(),
            character_maximum_lengths: UInt64Builder::with_capacity(default_capacity),
            character_octet_lengths: UInt64Builder::with_capacity(default_capacity),
            numeric_precisions: UInt64Builder::with_capacity(default_capacity),
            numeric_precision_radixes: UInt64Builder::with_capacity(default_capacity),
            numeric_scales: UInt64Builder::with_capacity(default_capacity),
            datetime_precisions: UInt64Builder::with_capacity(default_capacity),
            interval_types: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaColumns {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
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

/// Builds the `information_schema.COLUMNS` table row by row
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-columns.html>
struct InformationSchemaColumnsBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    column_names: StringBuilder,
    ordinal_positions: UInt64Builder,
    column_defaults: StringBuilder,
    is_nullables: StringBuilder,
    data_types: StringBuilder,
    character_maximum_lengths: UInt64Builder,
    character_octet_lengths: UInt64Builder,
    numeric_precisions: UInt64Builder,
    numeric_precision_radixes: UInt64Builder,
    numeric_scales: UInt64Builder,
    datetime_precisions: UInt64Builder,
    interval_types: StringBuilder,
}

impl InformationSchemaColumnsBuilder {
    fn add_column(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        field_position: usize,
        field: &Field,
    ) {
        use DataType::*;

        // Note: append_value is actually infallible.
        self.catalog_names.append_value(catalog_name);
        self.schema_names.append_value(schema_name);
        self.table_names.append_value(table_name);

        self.column_names.append_value(field.name());

        self.ordinal_positions.append_value(field_position as u64);

        // DataFusion does not support column default values, so null
        self.column_defaults.append_null();

        // "YES if the column is possibly nullable, NO if it is known not nullable. "
        let nullable_str = if field.is_nullable() { "YES" } else { "NO" };
        self.is_nullables.append_value(nullable_str);

        // "System supplied type" --> Use debug format of the datatype
        self.data_types.append_value(field.data_type().to_string());

        // "If data_type identifies a character or bit string type, the
        // declared maximum length; null for all other data types or
        // if no maximum length was declared."
        //
        // Arrow has no equivalent of VARCHAR(20), so we leave this as Null
        let max_chars = None;
        self.character_maximum_lengths.append_option(max_chars);

        // "Maximum length, in bytes, for binary data, character data,
        // or text and image data."
        let char_len: Option<u64> = match field.data_type() {
            Utf8 | Binary => Some(i32::MAX as u64),
            LargeBinary | LargeUtf8 => Some(i64::MAX as u64),
            _ => None,
        };
        self.character_octet_lengths.append_option(char_len);

        // numeric_precision: "If data_type identifies a numeric type, this column
        // contains the (declared or implicit) precision of the type
        // for this column. The precision indicates the number of
        // significant digits. It can be expressed in decimal (base
        // 10) or binary (base 2) terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null."
        //
        // numeric_radix: If data_type identifies a numeric type, this
        // column indicates in which base the values in the columns
        // numeric_precision and numeric_scale are expressed. The
        // value is either 2 or 10. For all other data types, this
        // column is null.
        //
        // numeric_scale: If data_type identifies an exact numeric
        // type, this column contains the (declared or implicit) scale
        // of the type for this column. The scale indicates the number
        // of significant digits to the right of the decimal point. It
        // can be expressed in decimal (base 10) or binary (base 2)
        // terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null.
        let (numeric_precision, numeric_radix, numeric_scale) = match field.data_type() {
            Int8 | UInt8 => (Some(8), Some(2), None),
            Int16 | UInt16 => (Some(16), Some(2), None),
            Int32 | UInt32 => (Some(32), Some(2), None),
            // From max value of 65504 as explained on
            // https://en.wikipedia.org/wiki/Half-precision_floating-point_format#Exponent_encoding
            Float16 => (Some(15), Some(2), None),
            // Numbers from postgres `real` type
            Float32 => (Some(24), Some(2), None),
            // Numbers from postgres `double` type
            Float64 => (Some(24), Some(2), None),
            Decimal128(precision, scale) => {
                (Some(*precision as u64), Some(10), Some(*scale as u64))
            }
            _ => (None, None, None),
        };

        self.numeric_precisions.append_option(numeric_precision);
        self.numeric_precision_radixes.append_option(numeric_radix);
        self.numeric_scales.append_option(numeric_scale);

        self.datetime_precisions.append_option(None);
        self.interval_types.append_null();
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.column_names.finish()),
                Arc::new(self.ordinal_positions.finish()),
                Arc::new(self.column_defaults.finish()),
                Arc::new(self.is_nullables.finish()),
                Arc::new(self.data_types.finish()),
                Arc::new(self.character_maximum_lengths.finish()),
                Arc::new(self.character_octet_lengths.finish()),
                Arc::new(self.numeric_precisions.finish()),
                Arc::new(self.numeric_precision_radixes.finish()),
                Arc::new(self.numeric_scales.finish()),
                Arc::new(self.datetime_precisions.finish()),
                Arc::new(self.interval_types.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Debug)]
struct InformationSchemata {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemata {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("schema_owner", DataType::Utf8, true),
            Field::new("default_character_set_catalog", DataType::Utf8, true),
            Field::new("default_character_set_schema", DataType::Utf8, true),
            Field::new("default_character_set_name", DataType::Utf8, true),
            Field::new("sql_path", DataType::Utf8, true),
        ]));
        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemataBuilder {
        InformationSchemataBuilder {
            schema: Arc::clone(&self.schema),
            catalog_name: StringBuilder::new(),
            schema_name: StringBuilder::new(),
            schema_owner: StringBuilder::new(),
            default_character_set_catalog: StringBuilder::new(),
            default_character_set_schema: StringBuilder::new(),
            default_character_set_name: StringBuilder::new(),
            sql_path: StringBuilder::new(),
        }
    }
}

struct InformationSchemataBuilder {
    schema: SchemaRef,
    catalog_name: StringBuilder,
    schema_name: StringBuilder,
    schema_owner: StringBuilder,
    default_character_set_catalog: StringBuilder,
    default_character_set_schema: StringBuilder,
    default_character_set_name: StringBuilder,
    sql_path: StringBuilder,
}

impl InformationSchemataBuilder {
    fn add_schemata(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        schema_owner: Option<&str>,
    ) {
        self.catalog_name.append_value(catalog_name);
        self.schema_name.append_value(schema_name);
        match schema_owner {
            Some(owner) => self.schema_owner.append_value(owner),
            None => self.schema_owner.append_null(),
        }
        // refer to https://www.postgresql.org/docs/current/infoschema-schemata.html,
        // these rows apply to a feature that is not implemented in DataFusion
        self.default_character_set_catalog.append_null();
        self.default_character_set_schema.append_null();
        self.default_character_set_name.append_null();
        self.sql_path.append_null();
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.catalog_name.finish()),
                Arc::new(self.schema_name.finish()),
                Arc::new(self.schema_owner.finish()),
                Arc::new(self.default_character_set_catalog.finish()),
                Arc::new(self.default_character_set_schema.finish()),
                Arc::new(self.default_character_set_name.finish()),
                Arc::new(self.sql_path.finish()),
            ],
        )
        .unwrap()
    }
}

impl PartitionStream for InformationSchemata {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_schemata(&mut builder).await;
                Ok(builder.finish())
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaDfSettingsBuilder {
        InformationSchemaDfSettingsBuilder {
            names: StringBuilder::new(),
            values: StringBuilder::new(),
            descriptions: StringBuilder::new(),
            schema: Arc::clone(&self.schema),
        }
    }
}

impl PartitionStream for InformationSchemaDfSettings {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = self.config.clone();
        let mut builder = self.builder();
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            // TODO: Stream this
            futures::stream::once(async move {
                // create a mem table with the names of tables
                let runtime_env = ctx.runtime_env();
                config.make_df_settings(
                    ctx.session_config().options(),
                    &runtime_env,
                    &mut builder,
                );
                Ok(builder.finish())
            }),
        ))
    }
}

struct InformationSchemaDfSettingsBuilder {
    schema: SchemaRef,
    names: StringBuilder,
    values: StringBuilder,
    descriptions: StringBuilder,
}

impl InformationSchemaDfSettingsBuilder {
    fn add_setting(&mut self, entry: ConfigEntry) {
        self.names.append_value(entry.key);
        self.values.append_option(entry.value);
        self.descriptions.append_value(entry.description);
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.names.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.descriptions.finish()),
            ],
        )
        .unwrap()
    }
}

#[derive(Debug)]
struct InformationSchemaRoutines {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaRoutines {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("specific_catalog", DataType::Utf8, false),
            Field::new("specific_schema", DataType::Utf8, false),
            Field::new("specific_name", DataType::Utf8, false),
            Field::new("routine_catalog", DataType::Utf8, false),
            Field::new("routine_schema", DataType::Utf8, false),
            Field::new("routine_name", DataType::Utf8, false),
            Field::new("routine_type", DataType::Utf8, false),
            Field::new("is_deterministic", DataType::Boolean, true),
            Field::new("data_type", DataType::Utf8, true),
            Field::new("function_type", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("syntax_example", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaRoutinesBuilder {
        InformationSchemaRoutinesBuilder {
            schema: Arc::clone(&self.schema),
            specific_catalog: StringBuilder::new(),
            specific_schema: StringBuilder::new(),
            specific_name: StringBuilder::new(),
            routine_catalog: StringBuilder::new(),
            routine_schema: StringBuilder::new(),
            routine_name: StringBuilder::new(),
            routine_type: StringBuilder::new(),
            is_deterministic: BooleanBuilder::new(),
            data_type: StringBuilder::new(),
            function_type: StringBuilder::new(),
            description: StringBuilder::new(),
            syntax_example: StringBuilder::new(),
        }
    }
}

struct InformationSchemaRoutinesBuilder {
    schema: SchemaRef,
    specific_catalog: StringBuilder,
    specific_schema: StringBuilder,
    specific_name: StringBuilder,
    routine_catalog: StringBuilder,
    routine_schema: StringBuilder,
    routine_name: StringBuilder,
    routine_type: StringBuilder,
    is_deterministic: BooleanBuilder,
    data_type: StringBuilder,
    function_type: StringBuilder,
    description: StringBuilder,
    syntax_example: StringBuilder,
}

impl InformationSchemaRoutinesBuilder {
    #[expect(clippy::too_many_arguments)]
    fn add_routine(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        routine_name: impl AsRef<str>,
        routine_type: impl AsRef<str>,
        is_deterministic: bool,
        data_type: Option<&impl AsRef<str>>,
        function_type: impl AsRef<str>,
        description: Option<impl AsRef<str>>,
        syntax_example: Option<impl AsRef<str>>,
    ) {
        self.specific_catalog.append_value(catalog_name.as_ref());
        self.specific_schema.append_value(schema_name.as_ref());
        self.specific_name.append_value(routine_name.as_ref());
        self.routine_catalog.append_value(catalog_name.as_ref());
        self.routine_schema.append_value(schema_name.as_ref());
        self.routine_name.append_value(routine_name.as_ref());
        self.routine_type.append_value(routine_type.as_ref());
        self.is_deterministic.append_value(is_deterministic);
        self.data_type.append_option(data_type.as_ref());
        self.function_type.append_value(function_type.as_ref());
        self.description.append_option(description);
        self.syntax_example.append_option(syntax_example);
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.specific_catalog.finish()),
                Arc::new(self.specific_schema.finish()),
                Arc::new(self.specific_name.finish()),
                Arc::new(self.routine_catalog.finish()),
                Arc::new(self.routine_schema.finish()),
                Arc::new(self.routine_name.finish()),
                Arc::new(self.routine_type.finish()),
                Arc::new(self.is_deterministic.finish()),
                Arc::new(self.data_type.finish()),
                Arc::new(self.function_type.finish()),
                Arc::new(self.description.finish()),
                Arc::new(self.syntax_example.finish()),
            ],
        )
        .unwrap()
    }
}

impl PartitionStream for InformationSchemaRoutines {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = self.config.clone();
        let mut builder = self.builder();
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("specific_catalog", DataType::Utf8, false),
            Field::new("specific_schema", DataType::Utf8, false),
            Field::new("specific_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::UInt64, false),
            Field::new("parameter_mode", DataType::Utf8, false),
            Field::new("parameter_name", DataType::Utf8, true),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("parameter_default", DataType::Utf8, true),
            Field::new("is_variadic", DataType::Boolean, false),
            // `rid` (short for `routine id`) is used to differentiate parameters from different signatures
            // (It serves as the group-by key when generating the `SHOW FUNCTIONS` query).
            // For example, the following signatures have different `rid` values:
            //     - `datetrunc(Utf8, Timestamp(Microsecond, Some("+TZ"))) -> Timestamp(Microsecond, Some("+TZ"))`
            //     - `datetrunc(Utf8View, Timestamp(Nanosecond, None)) -> Timestamp(Nanosecond, None)`
            Field::new("rid", DataType::UInt8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaParametersBuilder {
        InformationSchemaParametersBuilder {
            schema: Arc::clone(&self.schema),
            specific_catalog: StringBuilder::new(),
            specific_schema: StringBuilder::new(),
            specific_name: StringBuilder::new(),
            ordinal_position: UInt64Builder::new(),
            parameter_mode: StringBuilder::new(),
            parameter_name: StringBuilder::new(),
            data_type: StringBuilder::new(),
            parameter_default: StringBuilder::new(),
            is_variadic: BooleanBuilder::new(),
            rid: UInt8Builder::new(),
        }
    }
}

struct InformationSchemaParametersBuilder {
    schema: SchemaRef,
    specific_catalog: StringBuilder,
    specific_schema: StringBuilder,
    specific_name: StringBuilder,
    ordinal_position: UInt64Builder,
    parameter_mode: StringBuilder,
    parameter_name: StringBuilder,
    data_type: StringBuilder,
    parameter_default: StringBuilder,
    is_variadic: BooleanBuilder,
    rid: UInt8Builder,
}

impl InformationSchemaParametersBuilder {
    #[expect(clippy::too_many_arguments)]
    fn add_parameter(
        &mut self,
        specific_catalog: impl AsRef<str>,
        specific_schema: impl AsRef<str>,
        specific_name: impl AsRef<str>,
        ordinal_position: u64,
        parameter_mode: impl AsRef<str>,
        parameter_name: Option<&(impl AsRef<str> + ?Sized)>,
        data_type: impl AsRef<str>,
        parameter_default: Option<impl AsRef<str>>,
        is_variadic: bool,
        rid: u8,
    ) {
        self.specific_catalog
            .append_value(specific_catalog.as_ref());
        self.specific_schema.append_value(specific_schema.as_ref());
        self.specific_name.append_value(specific_name.as_ref());
        self.ordinal_position.append_value(ordinal_position);
        self.parameter_mode.append_value(parameter_mode.as_ref());
        self.parameter_name.append_option(parameter_name.as_ref());
        self.data_type.append_value(data_type.as_ref());
        self.parameter_default.append_option(parameter_default);
        self.is_variadic.append_value(is_variadic);
        self.rid.append_value(rid);
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(self.specific_catalog.finish()),
                Arc::new(self.specific_schema.finish()),
                Arc::new(self.specific_name.finish()),
                Arc::new(self.ordinal_position.finish()),
                Arc::new(self.parameter_mode.finish()),
                Arc::new(self.parameter_name.finish()),
                Arc::new(self.data_type.finish()),
                Arc::new(self.parameter_default.finish()),
                Arc::new(self.is_variadic.finish()),
                Arc::new(self.rid.finish()),
            ],
        )
        .unwrap()
    }
}

impl PartitionStream for InformationSchemaParameters {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = self.config.clone();
        let mut builder = self.builder();
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

    #[tokio::test]
    async fn make_tables_uses_table_type() {
        let config = InformationSchemaConfig {
            catalog_list: Arc::new(Fixture),
        };
        let mut builder = InformationSchemaTablesBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            table_types: StringBuilder::new(),
            schema: Arc::new(Schema::empty()),
        };

        assert!(config.make_tables(&mut builder).await.is_ok());

        assert_eq!("BASE TABLE", builder.table_types.finish().value(0));
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
            panic!("InformationSchemaConfig::make_tables called SchemaProvider::table instead of table_type")
        }

        fn as_any(&self) -> &dyn Any {
            unimplemented!("not required for these tests")
        }

        fn table_names(&self) -> Vec<String> {
            vec!["atable".to_string()]
        }

        fn table_exist(&self, _: &str) -> bool {
            unimplemented!("not required for these tests")
        }
    }

    impl CatalogProviderList for Fixture {
        fn as_any(&self) -> &dyn Any {
            unimplemented!("not required for these tests")
        }

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
        fn as_any(&self) -> &dyn Any {
            unimplemented!("not required for these tests")
        }

        fn schema_names(&self) -> Vec<String> {
            vec!["aschema".to_string()]
        }

        fn schema(&self, _: &str) -> Option<Arc<dyn SchemaProvider>> {
            Some(Arc::new(Self))
        }
    }
}
