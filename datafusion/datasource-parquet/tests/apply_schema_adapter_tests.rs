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

mod parquet_adapter_tests {
    use arrow::{
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use datafusion_common::{ColumnStatistics, DataFusionError, Result};
    use datafusion_datasource::{
        file::FileSource,
        file_scan_config::FileScanConfigBuilder,
        schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper},
    };
    use datafusion_datasource_parquet::source::ParquetSource;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use std::{fmt::Debug, sync::Arc};

    /// A test schema adapter factory that adds prefix to column names
    #[derive(Debug)]
    struct PrefixAdapterFactory {
        prefix: String,
    }

    impl SchemaAdapterFactory for PrefixAdapterFactory {
        fn create(
            &self,
            projected_table_schema: SchemaRef,
            _table_schema: SchemaRef,
        ) -> Box<dyn SchemaAdapter> {
            Box::new(PrefixAdapter {
                input_schema: projected_table_schema,
                prefix: self.prefix.clone(),
            })
        }
    }

    /// A test schema adapter that adds prefix to column names
    #[derive(Debug)]
    struct PrefixAdapter {
        input_schema: SchemaRef,
        prefix: String,
    }

    impl SchemaAdapter for PrefixAdapter {
        fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
            let field = self.input_schema.field(index);
            file_schema.fields.find(field.name()).map(|(i, _)| i)
        }

        fn map_schema(
            &self,
            file_schema: &Schema,
        ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
            let mut projection = Vec::with_capacity(file_schema.fields().len());
            for (file_idx, file_field) in file_schema.fields().iter().enumerate() {
                if self.input_schema.fields().find(file_field.name()).is_some() {
                    projection.push(file_idx);
                }
            }

            // Create a schema mapper that adds a prefix to column names
            #[derive(Debug)]
            struct PrefixSchemaMapping {
                // Keep only the prefix field which is actually used in the implementation
                prefix: String,
            }

            impl SchemaMapper for PrefixSchemaMapping {
                fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
                    // Create a new schema with prefixed field names
                    let prefixed_fields: Vec<Field> = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| {
                            Field::new(
                                format!("{}{}", self.prefix, field.name()),
                                field.data_type().clone(),
                                field.is_nullable(),
                            )
                        })
                        .collect();
                    let prefixed_schema = Arc::new(Schema::new(prefixed_fields));

                    // Create a new batch with the prefixed schema but the same data
                    let options = arrow::record_batch::RecordBatchOptions::default();
                    RecordBatch::try_new_with_options(
                        prefixed_schema,
                        batch.columns().to_vec(),
                        &options,
                    )
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                }

                fn map_column_statistics(
                    &self,
                    stats: &[ColumnStatistics],
                ) -> Result<Vec<ColumnStatistics>> {
                    // For testing, just return the input statistics
                    Ok(stats.to_vec())
                }
            }

            Ok((
                Arc::new(PrefixSchemaMapping {
                    prefix: self.prefix.clone(),
                }),
                projection,
            ))
        }
    }

    #[test]
    fn test_apply_schema_adapter_with_factory() {
        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create a parquet source
        let source = ParquetSource::default();

        // Create a file scan config with source that has a schema adapter factory
        let factory = Arc::new(PrefixAdapterFactory {
            prefix: "test_".to_string(),
        });

        let file_source = source.clone().with_schema_adapter_factory(factory).unwrap();

        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            schema.clone(),
            file_source,
        )
        .build();

        // Apply schema adapter to a new source
        let result_source = source.apply_schema_adapter(&config).unwrap();

        // Verify the adapter was applied
        assert!(result_source.schema_adapter_factory().is_some());

        // Create adapter and test it produces expected schema
        let adapter_factory = result_source.schema_adapter_factory().unwrap();
        let adapter = adapter_factory.create(schema.clone(), schema.clone());

        // Create a dummy batch to test the schema mapping
        let dummy_batch = RecordBatch::new_empty(schema.clone());

        // Get the file schema (which is the same as the table schema in this test)
        let (mapper, _) = adapter.map_schema(&schema).unwrap();

        // Apply the mapping to get the output schema
        let mapped_batch = mapper.map_batch(dummy_batch).unwrap();
        let output_schema = mapped_batch.schema();

        // Check the column names have the prefix
        assert_eq!(output_schema.field(0).name(), "test_id");
        assert_eq!(output_schema.field(1).name(), "test_name");
    }

    #[test]
    fn test_apply_schema_adapter_without_factory() {
        // Create a schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create a parquet source
        let source = ParquetSource::default();

        // Convert to Arc<dyn FileSource>
        let file_source: Arc<dyn FileSource> = Arc::new(source.clone());

        // Create a file scan config without a schema adapter factory
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            schema.clone(),
            file_source,
        )
        .build();

        // Apply schema adapter function - should pass through the source unchanged
        let result_source = source.apply_schema_adapter(&config).unwrap();

        // Verify no adapter was applied
        assert!(result_source.schema_adapter_factory().is_none());
    }
}
