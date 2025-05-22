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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::physical_plan::arrow_file::ArrowSource;
use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory};
use std::sync::Arc;

#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::ParquetSource;

#[cfg(feature = "avro")]
use datafusion_datasource_avro::AvroSource;

#[cfg(feature = "json")]
use datafusion_datasource_json::JsonSource;

#[cfg(feature = "csv")]
use datafusion_datasource_csv::CsvSource;

/// A test schema adapter factory that adds an extra column
#[derive(Debug)]
struct TestSchemaAdapterFactory {}

impl SchemaAdapterFactory for TestSchemaAdapterFactory {
    fn create(&self, schema: &Schema) -> Result<Box<dyn SchemaAdapter>> {
        Ok(Box::new(TestSchemaAdapter {
            input_schema: Arc::new(schema.clone()),
        }))
    }
}

/// A test schema adapter that adds a column
#[derive(Debug)]
struct TestSchemaAdapter {
    input_schema: SchemaRef,
}

impl SchemaAdapter for TestSchemaAdapter {
    fn adapt(
        &self,
        mut record_batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch> {
        // In a real adapter, we would transform the record batch here
        // For this test, we're just verifying the adapter was called correctly
        Ok(record_batch)
    }

    fn output_schema(&self) -> SchemaRef {
        // This creates an output schema with one additional column
        let mut fields = self.input_schema.fields().clone();
        fields.push(Field::new("adapted_column", DataType::Utf8, true));
        Arc::new(Schema::new(fields))
    }
}

// General function to test schema adapter factory functionality for any file source
fn test_generic_schema_adapter_factory<T: FileSource + Default>(file_type: &str) {
    let source = T::default();

    // Test that schema adapter factory is initially None
    assert!(source.schema_adapter_factory().is_none());

    // Add a schema adapter factory
    let factory = Arc::new(TestSchemaAdapterFactory {});
    let source_with_adapter = source.with_schema_adapter_factory(factory);

    // Verify schema adapter factory is now set
    assert!(source_with_adapter.schema_adapter_factory().is_some());

    // Check that file_type method returns the expected value
    assert_eq!(source_with_adapter.file_type(), file_type);
}

#[test]
fn test_arrow_source_schema_adapter_factory() {
    test_generic_schema_adapter_factory::<ArrowSource>("arrow");
}

#[cfg(feature = "parquet")]
#[test]
fn test_parquet_source_schema_adapter_factory() {
    test_generic_schema_adapter_factory::<ParquetSource>("parquet");
}

#[cfg(feature = "avro")]
#[test]
fn test_avro_source_schema_adapter_factory() {
    test_generic_schema_adapter_factory::<AvroSource>("avro");
}

#[cfg(feature = "json")]
#[test]
fn test_json_source_schema_adapter_factory() {
    test_generic_schema_adapter_factory::<JsonSource>("json");
}

#[cfg(feature = "csv")]
#[test]
fn test_csv_source_schema_adapter_factory() {
    test_generic_schema_adapter_factory::<CsvSource>("csv");
}

#[test]
fn test_file_source_conversion() {
    // Test the as_file_source function
    let arrow_source = ArrowSource::default();
    let file_source = datafusion_datasource::as_file_source(arrow_source);
    assert_eq!(file_source.file_type(), "arrow");

    // Test the From implementation for ArrowSource
    let arrow_source = ArrowSource::default();
    let file_source: Arc<dyn FileSource> = arrow_source.into();
    assert_eq!(file_source.file_type(), "arrow");
}

#[cfg(feature = "parquet")]
#[test]
fn test_apply_schema_adapter() {
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion_datasource::file_scan_config::{
        FileScanConfig, FileScanConfigBuilder,
    };
    use datafusion_datasource_parquet::file_format::apply_schema_adapter;

    // Create a test schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    // Create a basic FileScanConfig
    let config_builder = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("file:///path/to/parquet").unwrap(),
        schema.clone(),
    );

    // Create source and apply adapter
    let source = ParquetSource::default();
    let factory = Arc::new(TestSchemaAdapterFactory {});
    let file_source = source.with_schema_adapter_factory(factory);
    let config = config_builder.with_source(file_source).build();

    // Test that apply_schema_adapter preserves the adapter
    let source = ParquetSource::default();
    let result = apply_schema_adapter(source, &config);

    // Verify the schema adapter factory was preserved
    assert!(result.schema_adapter_factory().is_some());
}
