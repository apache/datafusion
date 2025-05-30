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

//! Integration test for schema adapter factory functionality

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::arrow_file::ArrowSource;
use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::PartitionedFile;
use std::sync::Arc;
use tempfile::TempDir;

#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::ParquetSource;
#[cfg(feature = "parquet")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "parquet")]
use parquet::file::properties::WriterProperties;

#[cfg(feature = "csv")]
use datafusion_datasource_csv::CsvSource;

/// A schema adapter factory that transforms column names to uppercase
#[derive(Debug)]
struct UppercaseAdapterFactory {}

impl SchemaAdapterFactory for UppercaseAdapterFactory {
    fn create(&self, schema: &Schema) -> Result<Box<dyn SchemaAdapter>> {
        Ok(Box::new(UppercaseAdapter {
            input_schema: Arc::new(schema.clone()),
        }))
    }
}

/// Schema adapter that transforms column names to uppercase
#[derive(Debug)]
struct UppercaseAdapter {
    input_schema: SchemaRef,
}

impl SchemaAdapter for UppercaseAdapter {
    fn adapt(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        // In a real adapter, we might transform the data too
        // For this test, we're just passing through the batch
        Ok(record_batch)
    }

    fn output_schema(&self) -> SchemaRef {
        let fields = self
            .input_schema
            .fields()
            .iter()
            .map(|f| {
                Field::new(
                    f.name().to_uppercase().as_str(),
                    f.data_type().clone(),
                    f.is_nullable(),
                )
            })
            .collect();

        Arc::new(Schema::new(fields))
    }
}

#[cfg(feature = "parquet")]
#[tokio::test]
async fn test_parquet_integration_with_schema_adapter() -> Result<()> {
    // Create a temporary directory for our test file
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");
    let file_path_str = file_path.to_str().unwrap();

    // Create test data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;

    // Write test parquet file
    let file = std::fs::File::create(file_path_str)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    // Create a session context
    let ctx = SessionContext::new();

    // Create a ParquetSource with the adapter factory
    let source = ParquetSource::default()
        .with_schema_adapter_factory(Arc::new(UppercaseAdapterFactory {}));

    // Create a scan config
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse(&format!("file://{}", file_path_str))?,
        schema.clone(),
    )
    .with_source(source)
    .build();

    // Create a data source executor
    let exec = DataSourceExec::from_data_source(config);

    // Collect results
    let task_ctx = ctx.task_ctx();
    let stream = exec.execute(0, task_ctx)?;
    let batches = datafusion::physical_plan::common::collect(stream).await?;

    // There should be one batch
    assert_eq!(batches.len(), 1);

    // Verify the schema has uppercase column names
    let result_schema = batches[0].schema();
    assert_eq!(result_schema.field(0).name(), "ID");
    assert_eq!(result_schema.field(1).name(), "NAME");

    Ok(())
}

#[tokio::test]
async fn test_multi_source_schema_adapter_reuse() -> Result<()> {
    // This test verifies that the same schema adapter factory can be reused
    // across different file source types. This is important for ensuring that:
    // 1. The schema adapter factory interface works uniformly across all source types
    // 2. The factory can be shared and cloned efficiently using Arc
    // 3. Various data source implementations correctly implement the schema adapter factory pattern

    // Create a test factory
    let factory = Arc::new(UppercaseAdapterFactory {});

    // Apply the same adapter to different source types
    let arrow_source =
        ArrowSource::default().with_schema_adapter_factory(factory.clone());

    #[cfg(feature = "parquet")]
    let parquet_source =
        ParquetSource::default().with_schema_adapter_factory(factory.clone());

    #[cfg(feature = "csv")]
    let csv_source = CsvSource::default().with_schema_adapter_factory(factory.clone());

    // Verify adapters were properly set
    assert!(arrow_source.schema_adapter_factory().is_some());

    #[cfg(feature = "parquet")]
    assert!(parquet_source.schema_adapter_factory().is_some());

    #[cfg(feature = "csv")]
    assert!(csv_source.schema_adapter_factory().is_some());

    Ok(())
}

// Helper function to test From<T> for Arc<dyn FileSource> implementations
fn test_from_impl<T: Into<Arc<dyn FileSource>> + Default>(expected_file_type: &str) {
    let source = T::default();
    let file_source: Arc<dyn FileSource> = source.into();
    assert_eq!(file_source.file_type(), expected_file_type);
}

#[test]
fn test_from_implementations() {
    // Test From implementation for various sources
    test_from_impl::<ArrowSource>("arrow");

    #[cfg(feature = "parquet")]
    test_from_impl::<ParquetSource>("parquet");

    #[cfg(feature = "csv")]
    test_from_impl::<CsvSource>("csv");

    #[cfg(feature = "json")]
    test_from_impl::<datafusion_datasource_json::JsonSource>("json");
}

/// A simple test schema adapter factory that doesn't modify the schema
#[derive(Debug)]
struct TestSchemaAdapterFactory {}

impl SchemaAdapterFactory for TestSchemaAdapterFactory {
    fn create(&self, schema: &Schema) -> Result<Box<dyn SchemaAdapter>> {
        Ok(Box::new(TestSchemaAdapter {
            input_schema: Arc::new(schema.clone()),
        }))
    }
}

/// A test schema adapter that passes through data unmodified
#[derive(Debug)]
struct TestSchemaAdapter {
    input_schema: SchemaRef,
}

impl SchemaAdapter for TestSchemaAdapter {
    fn adapt(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        // Just pass through the batch unmodified
        Ok(record_batch)
    }

    fn output_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

#[cfg(feature = "parquet")]
#[test]
fn test_schema_adapter_preservation() {
    // Create a test schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    // Create source with schema adapter factory
    let source = ParquetSource::default();
    let factory = Arc::new(TestSchemaAdapterFactory {});
    let file_source = source.with_schema_adapter_factory(factory);

    // Create a FileScanConfig with the source
    let config_builder =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), schema.clone())
            .with_source(file_source.clone())
            // Add a file to make it valid
            .with_file(PartitionedFile::new("test.parquet", 100));

    let config = config_builder.build();

    // Verify the schema adapter factory is present in the file source
    assert!(config.source().schema_adapter_factory().is_some());
}
