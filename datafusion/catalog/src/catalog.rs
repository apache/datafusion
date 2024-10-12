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
use std::fmt::Debug;
use std::sync::Arc;

pub use crate::schema::SchemaProvider;
use datafusion_common::not_impl_err;
use datafusion_common::Result;

/// Represents a catalog, comprising a number of named schemas.
///
/// # Catalog Overview
///
/// To plan and execute queries, DataFusion needs a "Catalog" that provides
/// metadata such as which schemas and tables exist, their columns and data
/// types, and how to access the data.
///
/// The Catalog API consists:
/// * [`CatalogProviderList`]: a collection of `CatalogProvider`s
/// * [`CatalogProvider`]: a collection of `SchemaProvider`s (sometimes called a "database" in other systems)
/// * [`SchemaProvider`]:  a collection of `TableProvider`s (often called a "schema" in other systems)
/// * [`TableProvider`]:  individual tables
///
/// # Implementing Catalogs
///
/// To implement a catalog, you implement at least one of the [`CatalogProviderList`],
/// [`CatalogProvider`] and [`SchemaProvider`] traits and register them
/// appropriately in the `SessionContext`.
///
/// DataFusion comes with a simple in-memory catalog implementation,
/// `MemoryCatalogProvider`, that is used by default and has no persistence.
/// DataFusion does not include more complex Catalog implementations because
/// catalog management is a key design choice for most data systems, and thus
/// it is unlikely that any general-purpose catalog implementation will work
/// well across many use cases.
///
/// # Implementing "Remote" catalogs
///
/// Sometimes catalog information is stored remotely and requires a network call
/// to retrieve. For example, the [Delta Lake] table format stores table
/// metadata in files on S3 that must be first downloaded to discover what
/// schemas and tables exist.
///
/// [Delta Lake]: https://delta.io/
///
/// The [`CatalogProvider`] can support this use case, but it takes some care.
/// The planning APIs in DataFusion are not `async` and thus network IO can not
/// be performed "lazily" / "on demand" during query planning. The rationale for
/// this design is that using remote procedure calls for all catalog accesses
/// required for query planning would likely result in multiple network calls
/// per plan, resulting in very poor planning performance.
///
/// To implement [`CatalogProvider`] and [`SchemaProvider`] for remote catalogs,
/// you need to provide an in memory snapshot of the required metadata. Most
/// systems typically either already have this information cached locally or can
/// batch access to the remote catalog to retrieve multiple schemas and tables
/// in a single network call.
///
/// Note that [`SchemaProvider::table`] is an `async` function in order to
/// simplify implementing simple [`SchemaProvider`]s. For many table formats it
/// is easy to list all available tables but there is additional non trivial
/// access required to read table details (e.g. statistics).
///
/// The pattern that DataFusion itself uses to plan SQL queries is to walk over
/// the query to find all table references,
/// performing required remote catalog in parallel, and then plans the query
/// using that snapshot.
///
/// # Example Catalog Implementations
///
/// Here are some examples of how to implement custom catalogs:
///
/// * [`datafusion-cli`]: [`DynamicFileCatalogProvider`] catalog provider
///   that treats files and directories on a filesystem as tables.
///
/// * The [`catalog.rs`]:  a simple directory based catalog.
///
/// * [delta-rs]:  [`UnityCatalogProvider`] implementation that can
///   read from Delta Lake tables
///
/// [`datafusion-cli`]: https://datafusion.apache.org/user-guide/cli/index.html
/// [`DynamicFileCatalogProvider`]: https://github.com/apache/datafusion/blob/31b9b48b08592b7d293f46e75707aad7dadd7cbc/datafusion-cli/src/catalog.rs#L75
/// [`catalog.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/catalog.rs
/// [delta-rs]: https://github.com/delta-io/delta-rs
/// [`UnityCatalogProvider`]: https://github.com/delta-io/delta-rs/blob/951436ecec476ce65b5ed3b58b50fb0846ca7b91/crates/deltalake-core/src/data_catalog/unity/datafusion.rs#L111-L123
///
/// [`TableProvider`]: crate::TableProvider

pub trait CatalogProvider: Debug + Sync + Send {
    /// Returns the catalog provider as [`Any`]
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String>;

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    ///
    /// By default returns a "Not Implemented" error
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        // use variables to avoid unused variable warnings
        let _ = name;
        let _ = schema;
        not_impl_err!("Registering new schemas is not supported")
    }

    /// Removes a schema from this catalog. Implementations of this method should return
    /// errors if the schema exists but cannot be dropped. For example, in DataFusion's
    /// default in-memory catalog, `MemoryCatalogProvider`, a non-empty schema
    /// will only be successfully dropped when `cascade` is true.
    /// This is equivalent to how DROP SCHEMA works in PostgreSQL.
    ///
    /// Implementations of this method should return None if schema with `name`
    /// does not exist.
    ///
    /// By default returns a "Not Implemented" error
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        not_impl_err!("Deregistering new schemas is not supported")
    }
}

/// Represent a list of named [`CatalogProvider`]s.
///
/// Please see the documentation on `CatalogProvider` for details of
/// implementing a custom catalog.
pub trait CatalogProviderList: Debug + Sync + Send {
    /// Returns the catalog list as [`Any`]
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Adds a new catalog to this catalog list
    /// If a catalog of the same name existed before, it is replaced in the list and returned.
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>>;

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Vec<String>;

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;
}
