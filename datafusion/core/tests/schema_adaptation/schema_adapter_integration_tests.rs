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
use datafusion::datasource::physical_plan::ArrowSource;
use datafusion::datasource::physical_plan::JsonSource;
#[cfg(feature = "parquet")]
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::{
    FileOpener, FileScanConfig, FileScanConfigBuilder, FileSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Statistics;
use datafusion::prelude::*;
use datafusion_common::ColumnStatistics;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_datasource::schema_adapter::{
    SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
};
use datafusion_datasource::PartitionedFile;
use object_store::ObjectStore;
#[cfg(feature = "parquet")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "parquet")]
use parquet::file::properties::WriterProperties;
use std::any::Any;
use std::sync::Arc;
use tempfile::TempDir;

use datafusion::datasource::physical_plan::CsvSource;

/// A schema adapter factory that transforms column names to uppercase
#[derive(Debug)]
struct UppercaseAdapterFactory {}

impl SchemaAdapterFactory for UppercaseAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(UppercaseAdapter {
            table_schema: projected_table_schema,
        })
    }
}

/// Schema adapter that transforms column names to uppercase
#[derive(Debug)]
struct UppercaseAdapter {
    table_schema: SchemaRef,
}

impl SchemaAdapter for UppercaseAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        file_schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(field.name()))
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        for (idx, file_field) in file_schema.fields().iter().enumerate() {
            if self
                .table_schema
                .fields()
                .iter()
                .any(|f| f.name().eq_ignore_ascii_case(file_field.name()))
            {
                projection.push(idx);
            }
        }

        let mapper = UppercaseSchemaMapper {
            output_schema: self.output_schema(),
            projection: projection.clone(),
        };

        Ok((Arc::new(mapper), projection))
    }
}

#[derive(Debug)]
struct TestSchemaMapping {
    output_schema: SchemaRef,
    projection: Vec<usize>,
}

impl SchemaMapper for TestSchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .projection
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect::<Vec<_>>();
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }

    fn map_column_statistics(
        &self,
        stats: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        Ok(self
            .projection
            .iter()
            .map(|&i| stats.get(i).cloned().unwrap_or_default())
            .collect())
    }
}

impl UppercaseAdapter {
    #[allow(dead_code)]
    fn adapt(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        Ok(record_batch)
    }

    fn output_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .table_schema
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

#[derive(Debug)]
struct UppercaseSchemaMapper {
    output_schema: SchemaRef,
    projection: Vec<usize>,
}

impl SchemaMapper for UppercaseSchemaMapper {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .projection
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect::<Vec<_>>();
        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
    }

    fn map_column_statistics(
        &self,
        stats: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        Ok(self
            .projection
            .iter()
            .map(|&i| stats.get(i).cloned().unwrap_or_default())
            .collect())
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
    let file_source = ParquetSource::default()
        .with_schema_adapter_factory(Arc::new(UppercaseAdapterFactory {}))?;

    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse(format!("file://{file_path_str}"))?,
        schema.clone(),
        file_source.clone(),
    )
    .with_file(PartitionedFile::new(file_path_str, 100))
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
    assert_eq!(result_schema.field(1).name(), "NAME0");

    Ok(())
}

#[cfg(feature = "parquet")]
#[tokio::test]
async fn test_parquet_integration_with_schema_adapter_and_expression_rewriter(
) -> Result<()> {
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
    let file_source = ParquetSource::default()
        .with_schema_adapter_factory(Arc::new(UppercaseAdapterFactory {}))?;

    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse(format!("file://{file_path_str}"))?,
        schema.clone(),
        file_source,
    )
    .with_file(PartitionedFile::new(file_path_str, 100))
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
    let arrow_source = ArrowSource::default()
        .with_schema_adapter_factory(factory.clone())
        .unwrap();

    #[cfg(feature = "parquet")]
    let parquet_source = ParquetSource::default()
        .with_schema_adapter_factory(factory.clone())
        .unwrap();

    let csv_source = CsvSource::default()
        .with_schema_adapter_factory(factory.clone())
        .unwrap();

    // Verify adapters were properly set
    assert!(arrow_source.schema_adapter_factory().is_some());

    #[cfg(feature = "parquet")]
    assert!(parquet_source.schema_adapter_factory().is_some());

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

    test_from_impl::<CsvSource>("csv");

    test_from_impl::<JsonSource>("json");
}

/// A simple test schema adapter factory that doesn't modify the schema
#[derive(Debug)]
struct TestSchemaAdapterFactory {}

impl SchemaAdapterFactory for TestSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(TestSchemaAdapter {
            input_schema: projected_table_schema,
        })
    }
}

/// A test schema adapter that passes through data unmodified
#[derive(Debug)]
struct TestSchemaAdapter {
    input_schema: SchemaRef,
}

impl SchemaAdapter for TestSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.input_schema.field(index);
        file_schema
            .fields()
            .iter()
            .position(|f| f.name() == field.name())
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        for (idx, file_field) in file_schema.fields().iter().enumerate() {
            if self
                .input_schema
                .fields()
                .iter()
                .any(|f| f.name() == file_field.name())
            {
                projection.push(idx);
            }
        }

        let mapper = TestSchemaMapping {
            output_schema: Arc::clone(&self.input_schema),
            projection: projection.clone(),
        };

        Ok((Arc::new(mapper), projection))
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
    let file_source = source.with_schema_adapter_factory(factory).unwrap();

    // Create a FileScanConfig with the source
    let config_builder = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        schema.clone(),
        file_source.clone(),
    )
    .with_file(PartitionedFile::new("test.parquet", 100));

    let config = config_builder.build();

    // Verify the schema adapter factory is present in the file source
    assert!(config.file_source().schema_adapter_factory().is_some());
}

/// A test source for testing schema adapters
#[derive(Debug, Clone)]
struct TestSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    metrics: ExecutionPlanMetricsSet,
}

impl TestSource {
    fn new() -> Self {
        Self {
            schema_adapter_factory: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl FileSource for TestSource {
    fn file_type(&self) -> &str {
        "test"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create_file_opener(
        &self,
        _store: Arc<dyn ObjectStore>,
        _conf: &FileScanConfig,
        _index: usize,
    ) -> Arc<dyn FileOpener> {
        unimplemented!("Not needed for this test")
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_projection(&self, _projection: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_statistics(&self, _statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::default())
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

#[test]
fn test_schema_adapter() {
    // This test verifies the functionality of the SchemaAdapter and SchemaAdapterFactory
    // components used in DataFusion's file sources.
    //
    // The test specifically checks:
    // 1. Creating and attaching a schema adapter factory to a file source
    // 2. Creating a schema adapter using the factory
    // 3. The schema adapter's ability to map column indices between a table schema and a file schema
    // 4. The schema adapter's ability to create a projection that selects only the columns
    //    from the file schema that are present in the table schema
    //
    // Schema adapters are used when the schema of data in files doesn't exactly match
    // the schema expected by the query engine, allowing for field mapping and data transformation.

    // Create a test schema
    let table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    // Create a file schema
    let file_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("extra", DataType::Int64, true),
    ]);

    // Create a TestSource
    let source = TestSource::new();
    assert!(source.schema_adapter_factory().is_none());

    // Add a schema adapter factory
    let factory = Arc::new(TestSchemaAdapterFactory {});
    let source_with_adapter = source.with_schema_adapter_factory(factory).unwrap();
    assert!(source_with_adapter.schema_adapter_factory().is_some());

    // Create a schema adapter
    let adapter_factory = source_with_adapter.schema_adapter_factory().unwrap();
    let adapter =
        adapter_factory.create(Arc::clone(&table_schema), Arc::clone(&table_schema));

    // Test mapping column index
    assert_eq!(adapter.map_column_index(0, &file_schema), Some(0));
    assert_eq!(adapter.map_column_index(1, &file_schema), Some(1));

    // Test creating schema mapper
    let (_mapper, projection) = adapter.map_schema(&file_schema).unwrap();
    assert_eq!(projection, vec![0, 1]);
}
