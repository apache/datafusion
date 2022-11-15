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

use datafusion::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::common::ToDFSchema;
use datafusion::config::{
    ConfigOptions, OPT_PARQUET_ENABLE_PAGE_INDEX, OPT_PARQUET_PUSHDOWN_FILTERS,
    OPT_PARQUET_REORDER_FILTERS,
};
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::file_format::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use object_store::path::Path;
use object_store::ObjectMeta;
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
pub struct ParquetScanOptions {
    pub pushdown_filters: bool,
    pub reorder_filters: bool,
    pub enable_page_index: bool,
}

impl TestParquetFile {
    /// Creates a new parquet file at the specified location with the
    /// given properties
    pub fn try_new(
        path: PathBuf,
        props: WriterProperties,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<Self> {
        let file = File::create(&path).unwrap();

        let mut batches = batches.into_iter();
        let first_batch = batches.next().expect("need at least one record batch");
        let schema = first_batch.schema();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

        writer.write(&first_batch).unwrap();
        writer.flush()?;
        let mut num_rows = first_batch.num_rows();

        for batch in batches {
            writer.write(&batch).unwrap();
            writer.flush()?;
            num_rows += batch.num_rows();
        }
        writer.close().unwrap();

        println!("Generated test dataset with {} rows", num_rows);

        let size = std::fs::metadata(&path)?.len() as usize;

        let canonical_path = path.canonicalize()?;

        let object_store_url =
            ListingTableUrl::parse(canonical_path.to_str().unwrap_or_default())?
                .object_store();

        let object_meta = ObjectMeta {
            location: Path::parse(canonical_path.to_str().unwrap_or_default())?,
            last_modified: Default::default(),
            size,
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
    /// return a `ParquetExec` and `FilterExec` with the specified options to scan this parquet file.
    ///
    /// This returns the same plan that DataFusion will make with a pushed down predicate followed by a filter:
    ///
    /// ```text
    /// (FilterExec)
    ///   (ParquetExec)
    /// ```
    pub async fn create_scan(
        &self,
        filter: Expr,
        scan_options: ParquetScanOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ParquetScanOptions {
            pushdown_filters,
            reorder_filters,
            enable_page_index,
        } = scan_options;

        let mut config_options = ConfigOptions::new();
        config_options.set_bool(OPT_PARQUET_PUSHDOWN_FILTERS, pushdown_filters);
        config_options.set_bool(OPT_PARQUET_REORDER_FILTERS, reorder_filters);
        config_options.set_bool(OPT_PARQUET_ENABLE_PAGE_INDEX, enable_page_index);

        let scan_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: self.schema.clone(),
            file_groups: vec![vec![PartitionedFile {
                object_meta: self.object_meta.clone(),
                partition_values: vec![],
                range: None,
                extensions: None,
            }]],
            statistics: Default::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            config_options: config_options.into_shareable(),
            output_ordering: None,
        };

        let df_schema = self.schema.clone().to_dfschema_ref()?;

        // run coercion on the filters to coerce types etc.
        let props = ExecutionProps::new();
        let context = SimplifyContext::new(&props).with_schema(df_schema.clone());
        let simplifier = ExprSimplifier::new(context);
        let filter = simplifier.coerce(filter, df_schema.clone()).unwrap();

        let physical_filter_expr = create_physical_expr(
            &filter,
            &df_schema,
            self.schema.as_ref(),
            &ExecutionProps::default(),
        )?;

        let parquet_exec = Arc::new(ParquetExec::new(scan_config, Some(filter), None));

        let exec = Arc::new(FilterExec::try_new(physical_filter_expr, parquet_exec)?);

        Ok(exec)
    }

    /// Retrieve metrics from the parquet exec returned from `create_scan`
    ///
    /// Recursively searches for ParquetExec and returns the metrics
    /// on the first one it finds
    pub fn parquet_metrics(plan: Arc<dyn ExecutionPlan>) -> Option<MetricsSet> {
        if let Some(parquet) = plan.as_any().downcast_ref::<ParquetExec>() {
            return parquet.metrics();
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
        self.schema.clone()
    }

    /// The path to the parquet file
    pub fn path(&self) -> &std::path::Path {
        self.path.as_path()
    }
}
