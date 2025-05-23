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
use datafusion_common::ColumnStatistics;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory};
use std::sync::Arc;

// Import ArrowSource from the correct location
// ArrowSource is part of the core DataFusion package
use datafusion::datasource::physical_plan::ArrowSource;

#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::source::ParquetSource;

#[cfg(feature = "avro")]
use datafusion_datasource_avro::source::AvroSource;

// JSON and CSV sources are not available in the current feature set
// #[cfg(feature = "json")]
// use datafusion_datasource_json::JsonSource;

// #[cfg(feature = "csv")]
// use datafusion_datasource_csv::CsvSource;

/// A test schema adapter factory that adds an extra column
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

/// A test schema adapter that adds a column
#[derive(Debug)]
struct TestSchemaAdapter {
    input_schema: SchemaRef,
}

impl SchemaAdapter for TestSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.input_schema.field(index);
        file_schema.fields.find(field.name()).map(|(i, _)| i)
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(
        Arc<dyn datafusion_datasource::schema_adapter::SchemaMapper>,
        Vec<usize>,
    )> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        for (file_idx, file_field) in file_schema.fields().iter().enumerate() {
            if self.input_schema.fields().find(file_field.name()).is_some() {
                projection.push(file_idx);
            }
        }

        // Create a schema mapper that adds an adapted_column
        #[derive(Debug)]
        struct TestSchemaMapping {
            #[allow(dead_code)]
            input_schema: SchemaRef,
        }

        impl datafusion_datasource::schema_adapter::SchemaMapper for TestSchemaMapping {
            fn map_batch(
                &self,
                batch: arrow::record_batch::RecordBatch,
            ) -> datafusion_common::Result<arrow::record_batch::RecordBatch> {
                // In a real adapter, we would transform the record batch here
                // For this test, we're just verifying the adapter was called correctly
                Ok(batch)
            }

            fn map_column_statistics(
                &self,
                file_col_statistics: &[ColumnStatistics],
            ) -> datafusion_common::Result<Vec<ColumnStatistics>> {
                // For testing, just return the input statistics
                Ok(file_col_statistics.to_vec())
            }
        }

        Ok((
            Arc::new(TestSchemaMapping {
                input_schema: self.input_schema.clone(),
            }),
            projection,
        ))
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

// JSON and CSV sources are not available in the current feature set
// #[test]
// fn test_json_source_schema_adapter_factory() {
//     test_generic_schema_adapter_factory::<JsonSource>("json");
// }

// #[test]
// fn test_csv_source_schema_adapter_factory() {
//     test_generic_schema_adapter_factory::<CsvSource>("csv");
// }

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
fn test_schema_adapter_preservation() {
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;

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
    let config_builder = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("file:///path/to/parquet").unwrap(),
        schema.clone(),
        file_source.clone(),
    );

    let config = config_builder.build();

    // Verify the schema adapter factory is present in the file source
    assert!(config.file_source().schema_adapter_factory().is_some());
}
