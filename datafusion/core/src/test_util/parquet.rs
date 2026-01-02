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

//! Helpers for writing parquet files and reading them back

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crate::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use crate::common::ToDFSchema;
use crate::config::ConfigOptions;
use crate::datasource::listing::{ListingTableUrl, PartitionedFile};
use crate::datasource::object_store::ObjectStoreUrl;
use crate::datasource::physical_plan::ParquetSource;
use crate::error::Result;
use crate::logical_expr::execution_props::ExecutionProps;
use crate::logical_expr::simplify::SimplifyContext;
use crate::optimizer::simplify_expressions::ExprSimplifier;
use crate::physical_expr::create_physical_expr;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::metrics::MetricsSet;
use crate::prelude::{Expr, SessionConfig, SessionContext};

use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use object_store::ObjectMeta;
use object_store::path::Path;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

///  a ParquetFile that has been created for testing.
pub struct TestParquetFile {
    path: PathBuf,
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    object_meta: ObjectMeta,
}

#[derive(Debug, Clone, Copy)]
/// Options for how to create the parquet scan
pub struct ParquetScanOptions {
    /// Enable pushdown filters
    pub pushdown_filters: bool,
    /// enable reordering filters
    pub reorder_filters: bool,
    /// enable page index
    pub enable_page_index: bool,
}

impl ParquetScanOptions {
    /// Returns a [`SessionConfig`] with the given options
    pub fn config(&self) -> SessionConfig {
        let mut config = ConfigOptions::new();
        config.execution.parquet.pushdown_filters = self.pushdown_filters;
        config.execution.parquet.reorder_filters = self.reorder_filters;
        config.execution.parquet.enable_page_index = self.enable_page_index;
        config.into()
    }
}

impl TestParquetFile {
    /// Creates a new parquet file at the specified location with the
    /// given properties
    pub fn try_new(
        path: PathBuf,
        props: WriterProperties,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<Self> {
        let file = File::create(&path)?;

        let mut batches = batches.into_iter();
        let first_batch = batches.next().expect("need at least one record batch");
        let schema = first_batch.schema();

        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?;

        writer.write(&first_batch)?;
        let mut num_rows = first_batch.num_rows();

        for batch in batches {
            writer.write(&batch)?;
            num_rows += batch.num_rows();
        }
        writer.close()?;

        println!("Generated test dataset with {num_rows} rows");

        let size = std::fs::metadata(&path)?.len();

        let mut canonical_path = path.canonicalize()?;

        if cfg!(target_os = "windows") {
            canonical_path = canonical_path
                .to_str()
                .unwrap()
                .replace("\\", "/")
                .strip_prefix("//?/")
                .unwrap()
                .into();
        };

        let object_store_url =
            ListingTableUrl::parse(canonical_path.to_str().unwrap_or_default())?
                .object_store();

        let object_meta = ObjectMeta {
            location: Path::parse(canonical_path.to_str().unwrap_or_default())?,
            last_modified: Default::default(),
            size,
            e_tag: None,
            version: None,
        };

        Ok(Self {
            path,
            schema,
            object_store_url,
            object_meta,
        })
    }
}

impl TestParquetFile {
    /// Return a `DataSourceExec` with the specified options.
    ///
    /// If `maybe_filter` is non-None, the DataSourceExec will be filtered using
    /// the given expression, and this method will return the same plan that DataFusion
    /// will make with a pushed down predicate followed by a filter:
    ///
    /// ```text
    /// (FilterExec)
    ///   (DataSourceExec)
    /// ```
    ///
    /// Otherwise if `maybe_filter` is None, return just a `DataSourceExec`
    pub async fn create_scan(
        &self,
        ctx: &SessionContext,
        maybe_filter: Option<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let parquet_options = ctx.copied_table_options().parquet;
        let source = Arc::new(
            ParquetSource::new(Arc::clone(&self.schema))
                .with_table_parquet_options(parquet_options.clone()),
        );
        let scan_config_builder =
            FileScanConfigBuilder::new(self.object_store_url.clone(), source)
                .with_file(PartitionedFile::new_from_meta(self.object_meta.clone()));

        let df_schema = Arc::clone(&self.schema).to_dfschema_ref()?;

        // run coercion on the filters to coerce types etc.
        let props = ExecutionProps::new();
        let context = SimplifyContext::new(&props).with_schema(Arc::clone(&df_schema));
        if let Some(filter) = maybe_filter {
            let simplifier = ExprSimplifier::new(context);
            let filter = simplifier.coerce(filter, &df_schema).unwrap();
            let physical_filter_expr =
                create_physical_expr(&filter, &df_schema, &ExecutionProps::default())?;

            let source = Arc::new(
                ParquetSource::new(Arc::clone(&self.schema))
                    .with_table_parquet_options(parquet_options)
                    .with_predicate(Arc::clone(&physical_filter_expr)),
            );
            let config = scan_config_builder.with_source(source).build();
            let parquet_exec = DataSourceExec::from_data_source(config);

            let exec = Arc::new(FilterExec::try_new(physical_filter_expr, parquet_exec)?);
            Ok(exec)
        } else {
            let config = scan_config_builder.build();
            Ok(DataSourceExec::from_data_source(config))
        }
    }

    /// Retrieve metrics from the parquet exec returned from `create_scan`
    ///
    /// Recursively searches for DataSourceExec and returns the metrics
    /// on the first one it finds
    pub fn parquet_metrics(plan: &Arc<dyn ExecutionPlan>) -> Option<MetricsSet> {
        if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>()
            && data_source_exec
                .downcast_to_file_source::<ParquetSource>()
                .is_some()
        {
            return data_source_exec.metrics();
        }

        for child in plan.children() {
            if let Some(metrics) = Self::parquet_metrics(child) {
                return Some(metrics);
            }
        }
        None
    }

    /// The schema of this parquet file
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// The path to the parquet file
    pub fn path(&self) -> &std::path::Path {
        self.path.as_path()
    }
}

/// Specification for a sorting column in a Parquet file.
///
/// This is used by [`create_sorted_parquet_file`] to define the sort order
/// when creating test Parquet files with sorting metadata.
#[derive(Debug, Clone)]
pub struct SortColumnSpec {
    /// The column index in the schema (0-based)
    pub column_idx: usize,
    /// If true, the column is sorted in descending order
    pub descending: bool,
    /// If true, nulls come before non-null values
    pub nulls_first: bool,
}

impl SortColumnSpec {
    /// Create a new sort column specification
    pub fn new(column_idx: usize, descending: bool, nulls_first: bool) -> Self {
        Self {
            column_idx,
            descending,
            nulls_first,
        }
    }

    /// Create an ascending, nulls-first sort column
    pub fn asc_nulls_first(column_idx: usize) -> Self {
        Self::new(column_idx, false, true)
    }

    /// Create an ascending, nulls-last sort column
    pub fn asc_nulls_last(column_idx: usize) -> Self {
        Self::new(column_idx, false, false)
    }

    /// Create a descending, nulls-first sort column
    pub fn desc_nulls_first(column_idx: usize) -> Self {
        Self::new(column_idx, true, true)
    }

    /// Create a descending, nulls-last sort column
    pub fn desc_nulls_last(column_idx: usize) -> Self {
        Self::new(column_idx, true, false)
    }
}

/// Creates a test Parquet file with sorting_columns metadata.
///
/// This is useful for testing ordering inference from Parquet files.
///
/// # Arguments
/// * `path` - The path where the Parquet file will be written
/// * `batches` - The record batches to write to the file
/// * `sorting_columns` - The sorting column specifications (defines the sort order)
///
/// # Example
/// ```ignore
/// use datafusion::test_util::parquet::{create_sorted_parquet_file, SortColumnSpec};
///
/// let batches = vec![batch1, batch2];
/// let sorting = vec![
///     SortColumnSpec::asc_nulls_first(0),  // First column ascending
///     SortColumnSpec::desc_nulls_last(1),  // Second column descending
/// ];
/// let test_file = create_sorted_parquet_file(path, batches, sorting)?;
/// ```
pub fn create_sorted_parquet_file(
    path: PathBuf,
    batches: impl IntoIterator<Item = RecordBatch>,
    sorting_columns: Vec<SortColumnSpec>,
) -> Result<TestParquetFile> {
    use parquet::file::metadata::SortingColumn;

    let parquet_sorting_columns: Vec<SortingColumn> = sorting_columns
        .into_iter()
        .map(|spec| SortingColumn {
            column_idx: spec.column_idx as i32,
            descending: spec.descending,
            nulls_first: spec.nulls_first,
        })
        .collect();

    let props = WriterProperties::builder()
        .set_sorting_columns(Some(parquet_sorting_columns))
        .build();

    TestParquetFile::try_new(path, props, batches)
}
