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

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::{HashMap, TableReference, error::Result, not_impl_err};
use datafusion_execution::config::SessionConfig;

use crate::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};

/// A schema provider that looks up tables in a cache
///
/// Instances are created by the [`AsyncSchemaProvider::resolve`] method
#[derive(Debug)]
struct ResolvedSchemaProvider {
    owner_name: Option<String>,
    cached_tables: HashMap<String, Arc<dyn TableProvider>>,
}
#[async_trait]
impl SchemaProvider for ResolvedSchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        self.owner_name.as_deref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.cached_tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.cached_tables.get(name).cloned())
    }

    fn register_table(
        &self,
        name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!(
            "Attempt to register table '{name}' with ResolvedSchemaProvider which is not supported"
        )
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        not_impl_err!(
            "Attempt to deregister table '{name}' with ResolvedSchemaProvider which is not supported"
        )
    }

    fn table_exist(&self, name: &str) -> bool {
        self.cached_tables.contains_key(name)
    }
}

/// Helper class for building a [`ResolvedSchemaProvider`]
struct ResolvedSchemaProviderBuilder {
    owner_name: String,
    async_provider: Arc<dyn AsyncSchemaProvider>,
    cached_tables: HashMap<String, Option<Arc<dyn TableProvider>>>,
}
impl ResolvedSchemaProviderBuilder {
    fn new(owner_name: String, async_provider: Arc<dyn AsyncSchemaProvider>) -> Self {
        Self {
            owner_name,
            async_provider,
            cached_tables: HashMap::new(),
        }
    }

    async fn resolve_table(&mut self, table_name: &str) -> Result<()> {
        if !self.cached_tables.contains_key(table_name) {
            let resolved_table = self.async_provider.table(table_name).await?;
            self.cached_tables
                .insert(table_name.to_string(), resolved_table);
        }
        Ok(())
    }

    fn finish(self) -> Arc<dyn SchemaProvider> {
        let cached_tables = self
            .cached_tables
            .into_iter()
            .filter_map(|(key, maybe_value)| maybe_value.map(|value| (key, value)))
            .collect();
        Arc::new(ResolvedSchemaProvider {
            owner_name: Some(self.owner_name),
            cached_tables,
        })
    }
}

/// A catalog provider that looks up schemas in a cache
///
/// Instances are created by the [`AsyncCatalogProvider::resolve`] method
#[derive(Debug)]
struct ResolvedCatalogProvider {
    cached_schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}
impl CatalogProvider for ResolvedCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.cached_schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.cached_schemas.get(name).cloned()
    }
}

/// Helper class for building a [`ResolvedCatalogProvider`]
struct ResolvedCatalogProviderBuilder {
    cached_schemas: HashMap<String, Option<ResolvedSchemaProviderBuilder>>,
    async_provider: Arc<dyn AsyncCatalogProvider>,
}
impl ResolvedCatalogProviderBuilder {
    fn new(async_provider: Arc<dyn AsyncCatalogProvider>) -> Self {
        Self {
            cached_schemas: HashMap::new(),
            async_provider,
        }
    }
    fn finish(self) -> Arc<dyn CatalogProvider> {
        let cached_schemas = self
            .cached_schemas
            .into_iter()
            .filter_map(|(key, maybe_value)| {
                maybe_value.map(|value| (key, value.finish()))
            })
            .collect();
        Arc::new(ResolvedCatalogProvider { cached_schemas })
    }
}

/// A catalog provider list that looks up catalogs in a cache
///
/// Instances are created by the [`AsyncCatalogProviderList::resolve`] method
#[derive(Debug)]
struct ResolvedCatalogProviderList {
    cached_catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
}
impl CatalogProviderList for ResolvedCatalogProviderList {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unimplemented!("resolved providers cannot handle registration APIs")
    }

    fn catalog_names(&self) -> Vec<String> {
        self.cached_catalogs.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.cached_catalogs.get(name).cloned()
    }
}

/// A trait for schema providers that must resolve tables asynchronously
///
/// The [`SchemaProvider::table`] method _is_ asynchronous.  However, this is primarily for convenience and
/// it is not a good idea for this method to be slow as this will cause poor planning performance.
///
/// It is a better idea to resolve the tables once and cache them in memory for the duration of
/// planning.  This trait helps implement that pattern.
///
/// After implementing this trait you can call the [`AsyncSchemaProvider::resolve`] method to get an
/// `Arc<dyn SchemaProvider>` that contains a cached copy of the referenced tables.  The `resolve`
/// method can be slow and asynchronous as it is only called once, before planning.
///
/// See the [remote_catalog.rs] for an end to end example
///
/// [remote_catalog.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/data_io/remote_catalog.rs
#[async_trait]
pub trait AsyncSchemaProvider: Send + Sync {
    /// Lookup a table in the schema provider
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;
    /// Creates a cached provider that can be used to execute a query containing given references
    ///
    /// This method will walk through the references and look them up once, creating a cache of table
    /// providers.  This cache will be returned as a synchronous TableProvider that can be used to plan
    /// and execute a query containing the given references.
    ///
    /// This cache is intended to be short-lived for the execution of a single query.  There is no mechanism
    /// for refresh or eviction of stale entries.
    ///
    /// See the [`AsyncSchemaProvider`] documentation for additional details
    async fn resolve(
        &self,
        references: &[TableReference],
        config: &SessionConfig,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let mut cached_tables = HashMap::<String, Option<Arc<dyn TableProvider>>>::new();

        for reference in references {
            let ref_catalog_name = reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog);

            // Maybe this is a reference to some other catalog provided in another way
            if ref_catalog_name != catalog_name {
                continue;
            }

            let ref_schema_name = reference
                .schema()
                .unwrap_or(&config.options().catalog.default_schema);

            if ref_schema_name != schema_name {
                continue;
            }

            if !cached_tables.contains_key(reference.table()) {
                let resolved_table = self.table(reference.table()).await?;
                cached_tables.insert(reference.table().to_string(), resolved_table);
            }
        }

        let cached_tables = cached_tables
            .into_iter()
            .filter_map(|(key, maybe_value)| maybe_value.map(|value| (key, value)))
            .collect();

        Ok(Arc::new(ResolvedSchemaProvider {
            cached_tables,
            owner_name: Some(catalog_name.to_string()),
        }))
    }
}

/// A trait for catalog providers that must resolve schemas asynchronously
///
/// The [`CatalogProvider::schema`] method is synchronous because asynchronous operations should
/// not be used during planning.  This trait makes it easy to lookup schema references once and cache
/// them for future planning use.  See [`AsyncSchemaProvider`] for more details on motivation.

#[async_trait]
pub trait AsyncCatalogProvider: Send + Sync {
    /// Lookup a schema in the provider
    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>>;

    /// Creates a cached provider that can be used to execute a query containing given references
    ///
    /// This method will walk through the references and look them up once, creating a cache of schema
    /// providers (each with their own cache of table providers).  This cache will be returned as a
    /// synchronous CatalogProvider that can be used to plan and execute a query containing the given
    /// references.
    ///
    /// This cache is intended to be short-lived for the execution of a single query.  There is no mechanism
    /// for refresh or eviction of stale entries.
    async fn resolve(
        &self,
        references: &[TableReference],
        config: &SessionConfig,
        catalog_name: &str,
    ) -> Result<Arc<dyn CatalogProvider>> {
        let mut cached_schemas =
            HashMap::<String, Option<ResolvedSchemaProviderBuilder>>::new();

        for reference in references {
            let ref_catalog_name = reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog);

            // Maybe this is a reference to some other catalog provided in another way
            if ref_catalog_name != catalog_name {
                continue;
            }

            let schema_name = reference
                .schema()
                .unwrap_or(&config.options().catalog.default_schema);

            let schema = if let Some(schema) = cached_schemas.get_mut(schema_name) {
                schema
            } else {
                let resolved_schema = self.schema(schema_name).await?;
                let resolved_schema = resolved_schema.map(|resolved_schema| {
                    ResolvedSchemaProviderBuilder::new(
                        catalog_name.to_string(),
                        resolved_schema,
                    )
                });
                cached_schemas.insert(schema_name.to_string(), resolved_schema);
                cached_schemas.get_mut(schema_name).unwrap()
            };

            // If we can't find the catalog don't bother checking the table
            let Some(schema) = schema else { continue };

            schema.resolve_table(reference.table()).await?;
        }

        let cached_schemas = cached_schemas
            .into_iter()
            .filter_map(|(key, maybe_builder)| {
                maybe_builder.map(|schema_builder| (key, schema_builder.finish()))
            })
            .collect::<HashMap<_, _>>();

        Ok(Arc::new(ResolvedCatalogProvider { cached_schemas }))
    }
}

/// A trait for catalog provider lists that must resolve catalogs asynchronously
///
/// The [`CatalogProviderList::catalog`] method is synchronous because asynchronous operations should
/// not be used during planning.  This trait makes it easy to lookup catalog references once and cache
/// them for future planning use.  See [`AsyncSchemaProvider`] for more details on motivation.
#[async_trait]
pub trait AsyncCatalogProviderList: Send + Sync {
    /// Lookup a catalog in the provider
    async fn catalog(&self, name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>>;

    /// Creates a cached provider that can be used to execute a query containing given references
    ///
    /// This method will walk through the references and look them up once, creating a cache of catalog
    /// providers, schema providers, and table providers.  This cache will be returned as a
    /// synchronous CatalogProvider that can be used to plan and execute a query containing the given
    /// references.
    ///
    /// This cache is intended to be short-lived for the execution of a single query.  There is no mechanism
    /// for refresh or eviction of stale entries.
    async fn resolve(
        &self,
        references: &[TableReference],
        config: &SessionConfig,
    ) -> Result<Arc<dyn CatalogProviderList>> {
        let mut cached_catalogs =
            HashMap::<String, Option<ResolvedCatalogProviderBuilder>>::new();

        for reference in references {
            let catalog_name = reference
                .catalog()
                .unwrap_or(&config.options().catalog.default_catalog);

            // We will do three lookups here, one for the catalog, one for the schema, and one for the table
            // We cache the result (both found results and not-found results) to speed up future lookups
            //
            // Note that a cache-miss is not an error at this point.  We allow for the possibility that
            // other providers may supply the reference.
            //
            // If this is the only provider then a not-found error will be raised during planning when it can't
            // find the reference in the cache.

            let catalog = if let Some(catalog) = cached_catalogs.get_mut(catalog_name) {
                catalog
            } else {
                let resolved_catalog = self.catalog(catalog_name).await?;
                let resolved_catalog =
                    resolved_catalog.map(ResolvedCatalogProviderBuilder::new);
                cached_catalogs.insert(catalog_name.to_string(), resolved_catalog);
                cached_catalogs.get_mut(catalog_name).unwrap()
            };

            // If we can't find the catalog don't bother checking the schema / table
            let Some(catalog) = catalog else { continue };

            let schema_name = reference
                .schema()
                .unwrap_or(&config.options().catalog.default_schema);

            let schema = if let Some(schema) = catalog.cached_schemas.get_mut(schema_name)
            {
                schema
            } else {
                let resolved_schema = catalog.async_provider.schema(schema_name).await?;
                let resolved_schema = resolved_schema.map(|async_schema| {
                    ResolvedSchemaProviderBuilder::new(
                        catalog_name.to_string(),
                        async_schema,
                    )
                });
                catalog
                    .cached_schemas
                    .insert(schema_name.to_string(), resolved_schema);
                catalog.cached_schemas.get_mut(schema_name).unwrap()
            };

            // If we can't find the catalog don't bother checking the table
            let Some(schema) = schema else { continue };

            schema.resolve_table(reference.table()).await?;
        }

        // Build the cached catalog provider list
        let cached_catalogs = cached_catalogs
            .into_iter()
            .filter_map(|(key, maybe_builder)| {
                maybe_builder.map(|catalog_builder| (key, catalog_builder.finish()))
            })
            .collect::<HashMap<_, _>>();

        Ok(Arc::new(ResolvedCatalogProviderList { cached_catalogs }))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
    };

    use arrow::datatypes::SchemaRef;
    use async_trait::async_trait;
    use datafusion_common::{Statistics, TableReference, error::Result};
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr::{Expr, TableType};
    use datafusion_physical_plan::ExecutionPlan;

    use crate::{Session, TableProvider};

    use super::{AsyncCatalogProvider, AsyncCatalogProviderList, AsyncSchemaProvider};

    #[derive(Debug)]
    struct MockTableProvider {}
    #[async_trait]
    impl TableProvider for MockTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        /// Get a reference to the schema for this table
        fn schema(&self) -> SchemaRef {
            unimplemented!()
        }

        fn table_type(&self) -> TableType {
            unimplemented!()
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn statistics(&self) -> Option<Statistics> {
            unimplemented!()
        }
    }

    #[derive(Default)]
    struct MockAsyncSchemaProvider {
        lookup_count: AtomicU32,
    }

    const MOCK_CATALOG: &str = "mock_catalog";
    const MOCK_SCHEMA: &str = "mock_schema";
    const MOCK_TABLE: &str = "mock_table";

    #[async_trait]
    impl AsyncSchemaProvider for MockAsyncSchemaProvider {
        async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            self.lookup_count.fetch_add(1, Ordering::Release);
            if name == MOCK_TABLE {
                Ok(Some(Arc::new(MockTableProvider {})))
            } else {
                Ok(None)
            }
        }
    }

    fn test_config() -> SessionConfig {
        let mut config = SessionConfig::default();
        config.options_mut().catalog.default_catalog = MOCK_CATALOG.to_string();
        config.options_mut().catalog.default_schema = MOCK_SCHEMA.to_string();
        config
    }

    #[tokio::test]
    async fn test_async_schema_provider_resolve() {
        async fn check(
            refs: Vec<TableReference>,
            expected_lookup_count: u32,
            found_tables: &[&str],
            not_found_tables: &[&str],
        ) {
            let async_provider = MockAsyncSchemaProvider::default();
            let cached_provider = async_provider
                .resolve(&refs, &test_config(), MOCK_CATALOG, MOCK_SCHEMA)
                .await
                .unwrap();

            assert_eq!(
                async_provider.lookup_count.load(Ordering::Acquire),
                expected_lookup_count
            );

            for table_ref in found_tables {
                let table = cached_provider.table(table_ref).await.unwrap();
                assert!(table.is_some());
            }

            for table_ref in not_found_tables {
                assert!(cached_provider.table(table_ref).await.unwrap().is_none());
            }
        }

        // Basic full lookups
        check(
            vec![
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, MOCK_TABLE),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "not_exists"),
            ],
            2,
            &[MOCK_TABLE],
            &["not_exists"],
        )
        .await;

        // Catalog / schema mismatch doesn't even search
        check(
            vec![
                TableReference::full(MOCK_CATALOG, "foo", MOCK_TABLE),
                TableReference::full("foo", MOCK_SCHEMA, MOCK_TABLE),
            ],
            0,
            &[],
            &[MOCK_TABLE],
        )
        .await;

        // Both hits and misses cached
        check(
            vec![
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, MOCK_TABLE),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, MOCK_TABLE),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "not_exists"),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "not_exists"),
            ],
            2,
            &[MOCK_TABLE],
            &["not_exists"],
        )
        .await;
    }

    #[derive(Default)]
    struct MockAsyncCatalogProvider {
        lookup_count: AtomicU32,
    }

    #[async_trait]
    impl AsyncCatalogProvider for MockAsyncCatalogProvider {
        async fn schema(
            &self,
            name: &str,
        ) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
            self.lookup_count.fetch_add(1, Ordering::Release);
            if name == MOCK_SCHEMA {
                Ok(Some(Arc::new(MockAsyncSchemaProvider::default())))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn test_async_catalog_provider_resolve() {
        async fn check(
            refs: Vec<TableReference>,
            expected_lookup_count: u32,
            found_schemas: &[&str],
            not_found_schemas: &[&str],
        ) {
            let async_provider = MockAsyncCatalogProvider::default();
            let cached_provider = async_provider
                .resolve(&refs, &test_config(), MOCK_CATALOG)
                .await
                .unwrap();

            assert_eq!(
                async_provider.lookup_count.load(Ordering::Acquire),
                expected_lookup_count
            );

            for schema_ref in found_schemas {
                let schema = cached_provider.schema(schema_ref);
                assert!(schema.is_some());
            }

            for schema_ref in not_found_schemas {
                assert!(cached_provider.schema(schema_ref).is_none());
            }
        }

        // Basic full lookups
        check(
            vec![
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "x"),
                TableReference::full(MOCK_CATALOG, "not_exists", "x"),
            ],
            2,
            &[MOCK_SCHEMA],
            &["not_exists"],
        )
        .await;

        // Catalog mismatch doesn't even search
        check(
            vec![TableReference::full("foo", MOCK_SCHEMA, "x")],
            0,
            &[],
            &[MOCK_SCHEMA],
        )
        .await;

        // Both hits and misses cached
        check(
            vec![
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "x"),
                TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, "x"),
                TableReference::full(MOCK_CATALOG, "not_exists", "x"),
                TableReference::full(MOCK_CATALOG, "not_exists", "x"),
            ],
            2,
            &[MOCK_SCHEMA],
            &["not_exists"],
        )
        .await;
    }

    #[derive(Default)]
    struct MockAsyncCatalogProviderList {
        lookup_count: AtomicU32,
    }

    #[async_trait]
    impl AsyncCatalogProviderList for MockAsyncCatalogProviderList {
        async fn catalog(
            &self,
            name: &str,
        ) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
            self.lookup_count.fetch_add(1, Ordering::Release);
            if name == MOCK_CATALOG {
                Ok(Some(Arc::new(MockAsyncCatalogProvider::default())))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn test_async_catalog_provider_list_resolve() {
        async fn check(
            refs: Vec<TableReference>,
            expected_lookup_count: u32,
            found_catalogs: &[&str],
            not_found_catalogs: &[&str],
        ) {
            let async_provider = MockAsyncCatalogProviderList::default();
            let cached_provider =
                async_provider.resolve(&refs, &test_config()).await.unwrap();

            assert_eq!(
                async_provider.lookup_count.load(Ordering::Acquire),
                expected_lookup_count
            );

            for catalog_ref in found_catalogs {
                let catalog = cached_provider.catalog(catalog_ref);
                assert!(catalog.is_some());
            }

            for catalog_ref in not_found_catalogs {
                assert!(cached_provider.catalog(catalog_ref).is_none());
            }
        }

        // Basic full lookups
        check(
            vec![
                TableReference::full(MOCK_CATALOG, "x", "x"),
                TableReference::full("not_exists", "x", "x"),
            ],
            2,
            &[MOCK_CATALOG],
            &["not_exists"],
        )
        .await;

        // Both hits and misses cached
        check(
            vec![
                TableReference::full(MOCK_CATALOG, "x", "x"),
                TableReference::full(MOCK_CATALOG, "x", "x"),
                TableReference::full("not_exists", "x", "x"),
                TableReference::full("not_exists", "x", "x"),
            ],
            2,
            &[MOCK_CATALOG],
            &["not_exists"],
        )
        .await;
    }

    #[tokio::test]
    async fn test_defaults() {
        for table_ref in &[
            TableReference::full(MOCK_CATALOG, MOCK_SCHEMA, MOCK_TABLE),
            TableReference::partial(MOCK_SCHEMA, MOCK_TABLE),
            TableReference::bare(MOCK_TABLE),
        ] {
            let async_provider = MockAsyncCatalogProviderList::default();
            let cached_provider = async_provider
                .resolve(std::slice::from_ref(table_ref), &test_config())
                .await
                .unwrap();

            let catalog = cached_provider
                .catalog(table_ref.catalog().unwrap_or(MOCK_CATALOG))
                .unwrap();
            let schema = catalog
                .schema(table_ref.schema().unwrap_or(MOCK_SCHEMA))
                .unwrap();
            assert!(schema.table(table_ref.table()).await.unwrap().is_some());
        }
    }
}
