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

use crate::options::ListingOptions;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_catalog::Session;
use datafusion_common::{config_err, internal_err};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_compression_type::FileCompressionType;
#[expect(deprecated)]
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use std::str::FromStr;
use std::sync::Arc;

/// Indicates the source of the schema for a [`crate::ListingTable`]
// PartialEq required for assert_eq! in tests
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum SchemaSource {
    /// Schema is not yet set (initial state)
    #[default]
    Unset,
    /// Schema was inferred from first table_path
    Inferred,
    /// Schema was specified explicitly via with_schema
    Specified,
}

/// Configuration for creating a [`crate::ListingTable`]
///
/// # Schema Evolution Support
///
/// This configuration supports schema evolution through the optional
/// [`PhysicalExprAdapterFactory`]. You might want to override the default factory when you need:
///
/// - **Type coercion requirements**: When you need custom logic for converting between
///   different Arrow data types (e.g., Int32 ↔ Int64, Utf8 ↔ LargeUtf8)
/// - **Column mapping**: You need to map columns with a legacy name to a new name
/// - **Custom handling of missing columns**: By default they are filled in with nulls, but you may e.g. want to fill them in with `0` or `""`.
#[derive(Debug, Clone, Default)]
pub struct ListingTableConfig {
    /// Paths on the `ObjectStore` for creating [`crate::ListingTable`].
    /// They should share the same schema and object store.
    pub table_paths: Vec<ListingTableUrl>,
    /// Optional `SchemaRef` for the to be created [`crate::ListingTable`].
    ///
    /// See details on [`ListingTableConfig::with_schema`]
    pub file_schema: Option<SchemaRef>,
    /// Optional [`ListingOptions`] for the to be created [`crate::ListingTable`].
    ///
    /// See details on [`ListingTableConfig::with_listing_options`]
    pub options: Option<ListingOptions>,
    /// Tracks the source of the schema information
    pub(crate) schema_source: SchemaSource,
    /// Optional [`PhysicalExprAdapterFactory`] for creating physical expression adapters
    pub(crate) expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl ListingTableConfig {
    /// Creates new [`ListingTableConfig`] for reading the specified URL
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            table_paths: vec![table_path],
            ..Default::default()
        }
    }

    /// Creates new [`ListingTableConfig`] with multiple table paths.
    ///
    ///  See `ListingTableConfigExt::infer_options` for details on what happens with multiple paths
    pub fn new_with_multi_paths(table_paths: Vec<ListingTableUrl>) -> Self {
        Self {
            table_paths,
            ..Default::default()
        }
    }

    /// Returns the source of the schema for this configuration
    pub fn schema_source(&self) -> SchemaSource {
        self.schema_source
    }
    /// Set the `schema` for the overall [`crate::ListingTable`]
    ///
    /// [`crate::ListingTable`] will automatically coerce, when possible, the schema
    /// for individual files to match this schema.
    ///
    /// If a schema is not provided, it is inferred using
    /// [`Self::infer_schema`].
    ///
    /// If the schema is provided, it must contain only the fields in the file
    /// without the table partitioning columns.
    ///
    /// # Example: Specifying Table Schema
    /// ```rust
    /// # use std::sync::Arc;
    /// # use datafusion_catalog_listing::{ListingTableConfig, ListingOptions};
    /// # use datafusion_datasource::ListingTableUrl;
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
    /// # use arrow::datatypes::{Schema, Field, DataType};
    /// # let table_paths = ListingTableUrl::parse("file:///path/to/data").unwrap();
    /// # let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    ///     Field::new("name", DataType::Utf8, true),
    /// ]));
    ///
    /// let config = ListingTableConfig::new(table_paths)
    ///     .with_listing_options(listing_options)  // Set options first
    ///     .with_schema(schema);                    // Then set schema
    /// ```
    pub fn with_schema(self, schema: SchemaRef) -> Self {
        // Note: We preserve existing options state, but downstream code may expect
        // options to be set. Consider calling with_listing_options() or infer_options()
        // before operations that require options to be present.
        debug_assert!(
            self.options.is_some() || cfg!(test),
            "ListingTableConfig::with_schema called without options set. \
             Consider calling with_listing_options() or infer_options() first to avoid panics in downstream code."
        );

        Self {
            file_schema: Some(schema),
            schema_source: SchemaSource::Specified,
            ..self
        }
    }

    /// Add `listing_options` to [`ListingTableConfig`]
    ///
    /// If not provided, format and other options are inferred via
    /// `ListingTableConfigExt::infer_options`.
    ///
    /// # Example: Configuring Parquet Files with Custom Options
    /// ```rust
    /// # use std::sync::Arc;
    /// # use datafusion_catalog_listing::{ListingTableConfig, ListingOptions};
    /// # use datafusion_datasource::ListingTableUrl;
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
    /// # let table_paths = ListingTableUrl::parse("file:///path/to/data").unwrap();
    /// let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    ///     .with_file_extension(".parquet")
    ///     .with_collect_stat(true);
    ///
    /// let config = ListingTableConfig::new(table_paths).with_listing_options(options);
    /// // Configure file format and options
    /// ```
    pub fn with_listing_options(self, listing_options: ListingOptions) -> Self {
        // Note: This method properly sets options, but be aware that downstream
        // methods like infer_schema() and try_new() require both schema and options
        // to be set to function correctly.
        debug_assert!(
            !self.table_paths.is_empty() || cfg!(test),
            "ListingTableConfig::with_listing_options called without table_paths set. \
             Consider calling new() or new_with_multi_paths() first to establish table paths."
        );

        Self {
            options: Some(listing_options),
            ..self
        }
    }

    /// Returns a tuple of `(file_extension, optional compression_extension)`
    ///
    /// For example a path ending with blah.test.csv.gz returns `("csv", Some("gz"))`
    /// For example a path ending with blah.test.csv returns `("csv", None)`
    pub fn infer_file_extension_and_compression_type(
        path: &str,
    ) -> datafusion_common::Result<(String, Option<String>)> {
        let mut exts = path.rsplit('.');

        let split = exts.next().unwrap_or("");

        let file_compression_type = FileCompressionType::from_str(split)
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        if file_compression_type.is_compressed() {
            let split2 = exts.next().unwrap_or("");
            Ok((split2.to_string(), Some(split.to_string())))
        } else {
            Ok((split.to_string(), None))
        }
    }

    /// Infer the [`SchemaRef`] based on `table_path`s.
    ///
    /// This method infers the table schema using the first `table_path`.
    /// See [`ListingOptions::infer_schema`] for more details
    ///
    /// # Errors
    /// * if `self.options` is not set. See [`Self::with_listing_options`]
    pub async fn infer_schema(
        self,
        state: &dyn Session,
    ) -> datafusion_common::Result<Self> {
        match self.options {
            Some(options) => {
                let ListingTableConfig {
                    table_paths,
                    file_schema,
                    options: _,
                    schema_source,
                    expr_adapter_factory,
                } = self;

                let (schema, new_schema_source) = match file_schema {
                    Some(schema) => (schema, schema_source), // Keep existing source if schema exists
                    None => {
                        if let Some(url) = table_paths.first() {
                            (
                                options.infer_schema(state, url).await?,
                                SchemaSource::Inferred,
                            )
                        } else {
                            (Arc::new(Schema::empty()), SchemaSource::Inferred)
                        }
                    }
                };

                Ok(Self {
                    table_paths,
                    file_schema: Some(schema),
                    options: Some(options),
                    schema_source: new_schema_source,
                    expr_adapter_factory,
                })
            }
            None => internal_err!("No `ListingOptions` set for inferring schema"),
        }
    }

    /// Infer the partition columns from `table_paths`.
    ///
    /// # Errors
    /// * if `self.options` is not set. See [`Self::with_listing_options`]
    pub async fn infer_partitions_from_path(
        self,
        state: &dyn Session,
    ) -> datafusion_common::Result<Self> {
        match self.options {
            Some(options) => {
                let Some(url) = self.table_paths.first() else {
                    return config_err!("No table path found");
                };
                let partitions = options
                    .infer_partitions(state, url)
                    .await?
                    .into_iter()
                    .map(|col_name| {
                        (
                            col_name,
                            DataType::Dictionary(
                                Box::new(DataType::UInt16),
                                Box::new(DataType::Utf8),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let options = options.with_table_partition_cols(partitions);
                Ok(Self {
                    table_paths: self.table_paths,
                    file_schema: self.file_schema,
                    options: Some(options),
                    schema_source: self.schema_source,
                    expr_adapter_factory: self.expr_adapter_factory,
                })
            }
            None => config_err!("No `ListingOptions` set for inferring schema"),
        }
    }

    /// Set the [`PhysicalExprAdapterFactory`] for the [`crate::ListingTable`]
    ///
    /// The expression adapter factory is used to create physical expression adapters that can
    /// handle schema evolution and type conversions when evaluating expressions
    /// with different schemas than the table schema.
    pub fn with_expr_adapter_factory(
        self,
        expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Self {
        Self {
            expr_adapter_factory: Some(expr_adapter_factory),
            ..self
        }
    }

    /// Deprecated: Set the [`SchemaAdapterFactory`] for the [`crate::ListingTable`]
    ///
    /// `SchemaAdapterFactory` has been removed. Use [`Self::with_expr_adapter_factory`]
    /// and `PhysicalExprAdapterFactory` instead. See `upgrading.md` for more details.
    ///
    /// This method is a no-op and returns `self` unchanged.
    #[deprecated(
        since = "52.0.0",
        note = "SchemaAdapterFactory has been removed. Use with_expr_adapter_factory and PhysicalExprAdapterFactory instead. See upgrading.md for more details."
    )]
    #[expect(deprecated)]
    pub fn with_schema_adapter_factory(
        self,
        _schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        // No-op - just return self unchanged
        self
    }
}
