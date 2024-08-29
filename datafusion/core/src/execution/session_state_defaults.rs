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

use crate::catalog::{CatalogProvider, TableProviderFactory};
use crate::catalog_common::listing_schema::ListingSchemaProvider;
use crate::catalog_common::{MemoryCatalogProvider, MemorySchemaProvider};
use crate::datasource::file_format::arrow::ArrowFormatFactory;
use crate::datasource::file_format::avro::AvroFormatFactory;
use crate::datasource::file_format::csv::CsvFormatFactory;
use crate::datasource::file_format::json::JsonFormatFactory;
#[cfg(feature = "parquet")]
use crate::datasource::file_format::parquet::ParquetFormatFactory;
use crate::datasource::file_format::FileFormatFactory;
use crate::datasource::provider::DefaultTableFactory;
use crate::execution::context::SessionState;
#[cfg(feature = "nested_expressions")]
use crate::functions_nested;
use crate::{functions, functions_aggregate, functions_window};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

/// Defaults that are used as part of creating a SessionState such as table providers,
/// file formats, registering of builtin functions, etc.
pub struct SessionStateDefaults {}

impl SessionStateDefaults {
    /// returns a map of the default [`TableProviderFactory`]s
    pub fn default_table_factories() -> HashMap<String, Arc<dyn TableProviderFactory>> {
        let mut table_factories: HashMap<String, Arc<dyn TableProviderFactory>> =
            HashMap::new();
        #[cfg(feature = "parquet")]
        table_factories.insert("PARQUET".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("CSV".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("JSON".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("NDJSON".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("AVRO".into(), Arc::new(DefaultTableFactory::new()));
        table_factories.insert("ARROW".into(), Arc::new(DefaultTableFactory::new()));

        table_factories
    }

    /// returns the default MemoryCatalogProvider
    pub fn default_catalog(
        config: &SessionConfig,
        table_factories: &HashMap<String, Arc<dyn TableProviderFactory>>,
        runtime: &Arc<RuntimeEnv>,
    ) -> MemoryCatalogProvider {
        let default_catalog = MemoryCatalogProvider::new();

        default_catalog
            .register_schema(
                &config.options().catalog.default_schema,
                Arc::new(MemorySchemaProvider::new()),
            )
            .expect("memory catalog provider can register schema");

        Self::register_default_schema(config, table_factories, runtime, &default_catalog);

        default_catalog
    }

    /// returns the list of default [`ExprPlanner`]s
    pub fn default_expr_planners() -> Vec<Arc<dyn ExprPlanner>> {
        let expr_planners: Vec<Arc<dyn ExprPlanner>> = vec![
            Arc::new(functions::core::planner::CoreFunctionPlanner::default()),
            // register crate of nested expressions (if enabled)
            #[cfg(feature = "nested_expressions")]
            Arc::new(functions_nested::planner::NestedFunctionPlanner),
            #[cfg(feature = "nested_expressions")]
            Arc::new(functions_nested::planner::FieldAccessPlanner),
            #[cfg(any(
                feature = "datetime_expressions",
                feature = "unicode_expressions"
            ))]
            Arc::new(functions::planner::UserDefinedFunctionPlanner),
        ];

        expr_planners
    }

    /// returns the list of default [`ScalarUDF']'s
    pub fn default_scalar_functions() -> Vec<Arc<ScalarUDF>> {
        #[cfg_attr(not(feature = "nested_expressions"), allow(unused_mut))]
        let mut functions: Vec<Arc<ScalarUDF>> = functions::all_default_functions();

        #[cfg(feature = "nested_expressions")]
        functions.append(&mut functions_nested::all_default_nested_functions());

        functions
    }

    /// returns the list of default [`AggregateUDF']'s
    pub fn default_aggregate_functions() -> Vec<Arc<AggregateUDF>> {
        functions_aggregate::all_default_aggregate_functions()
    }

    /// returns the list of default [`WindowUDF']'s
    pub fn default_window_functions() -> Vec<Arc<WindowUDF>> {
        functions_window::all_default_window_functions()
    }

    /// returns the list of default [`FileFormatFactory']'s
    pub fn default_file_formats() -> Vec<Arc<dyn FileFormatFactory>> {
        let file_formats: Vec<Arc<dyn FileFormatFactory>> = vec![
            #[cfg(feature = "parquet")]
            Arc::new(ParquetFormatFactory::new()),
            Arc::new(JsonFormatFactory::new()),
            Arc::new(CsvFormatFactory::new()),
            Arc::new(ArrowFormatFactory::new()),
            Arc::new(AvroFormatFactory::new()),
        ];

        file_formats
    }

    /// registers all builtin functions - scalar, array and aggregate
    pub fn register_builtin_functions(state: &mut SessionState) {
        Self::register_scalar_functions(state);
        Self::register_array_functions(state);
        Self::register_aggregate_functions(state);
    }

    /// registers all the builtin scalar functions
    pub fn register_scalar_functions(state: &mut SessionState) {
        functions::register_all(state).expect("can not register built in functions");
    }

    /// registers all the builtin array functions
    #[cfg_attr(not(feature = "nested_expressions"), allow(unused_variables))]
    pub fn register_array_functions(state: &mut SessionState) {
        // register crate of array expressions (if enabled)
        #[cfg(feature = "nested_expressions")]
        functions_nested::register_all(state)
            .expect("can not register nested expressions");
    }

    /// registers all the builtin aggregate functions
    pub fn register_aggregate_functions(state: &mut SessionState) {
        functions_aggregate::register_all(state)
            .expect("can not register aggregate functions");
    }

    /// registers the default schema
    pub fn register_default_schema(
        config: &SessionConfig,
        table_factories: &HashMap<String, Arc<dyn TableProviderFactory>>,
        runtime: &Arc<RuntimeEnv>,
        default_catalog: &MemoryCatalogProvider,
    ) {
        let url = config.options().catalog.location.as_ref();
        let format = config.options().catalog.format.as_ref();
        let (url, format) = match (url, format) {
            (Some(url), Some(format)) => (url, format),
            _ => return,
        };
        let url = url.to_string();
        let format = format.to_string();

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
        let factory = match table_factories.get(format.as_str()) {
            Some(factory) => factory,
            _ => return,
        };
        let schema =
            ListingSchemaProvider::new(authority, path, factory.clone(), store, format);
        let _ = default_catalog
            .register_schema("default", Arc::new(schema))
            .expect("Failed to register default schema");
    }

    /// registers the default [`FileFormatFactory`]s
    pub fn register_default_file_formats(state: &mut SessionState) {
        let formats = SessionStateDefaults::default_file_formats();
        for format in formats {
            if let Err(e) = state.register_file_format(format, false) {
                log::info!("Unable to register default file format: {e}")
            };
        }
    }
}
