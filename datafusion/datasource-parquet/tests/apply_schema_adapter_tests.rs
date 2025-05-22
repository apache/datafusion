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

#[cfg(feature = "parquet")]
mod parquet_adapter_tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion_common::Result;
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_scan_config::{
        FileScanConfig, FileScanConfigBuilder,
    };
    use datafusion_datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory};
    use datafusion_datasource_parquet::file_format::apply_schema_adapter;
    use datafusion_datasource_parquet::ParquetSource;
    use std::sync::Arc;

    /// A test schema adapter factory that adds prefix to column names
    #[derive(Debug)]
    struct PrefixAdapterFactory {
        prefix: String,
    }

    impl SchemaAdapterFactory for PrefixAdapterFactory {
        fn create(&self, schema: &Schema) -> Result<Box<dyn SchemaAdapter>> {
            Ok(Box::new(PrefixAdapter {
                input_schema: Arc::new(schema.clone()),
                prefix: self.prefix.clone(),
            }))
        }
    }

    /// A test schema adapter that adds prefix to column names
    #[derive(Debug)]
    struct PrefixAdapter {
        input_schema: SchemaRef,
        prefix: String,
    }

    impl SchemaAdapter for PrefixAdapter {
        fn adapt(
            &self,
            record_batch: arrow::record_batch::RecordBatch,
        ) -> Result<arrow::record_batch::RecordBatch> {
            // In a real adapter, we might transform the data
            // For this test, we're just verifying the adapter was called correctly
            Ok(record_batch)
        }

        fn output_schema(&self) -> SchemaRef {
            let fields = self
                .input_schema
                .fields()
                .iter()
                .map(|f| {
                    Field::new(
                        format!("{}{}", self.prefix, f.name()).as_str(),
                        f.data_type().clone(),
                        f.is_nullable(),
                    )
                })
                .collect();

            Arc::new(Schema::new(fields))
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

        let file_source = source.clone().with_schema_adapter_factory(factory);

        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            schema.clone(),
        )
        .with_source(file_source)
        .build();

        // Apply schema adapter to a new source
        let result_source = apply_schema_adapter(source, &config);

        // Verify the adapter was applied
        assert!(result_source.schema_adapter_factory().is_some());

        // Create adapter and test it produces expected schema
        let adapter_factory = result_source.schema_adapter_factory().unwrap();
        let adapter = adapter_factory.create(&schema).unwrap();
        let output_schema = adapter.output_schema();

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

        // Create a file scan config without a schema adapter factory
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            schema.clone(),
        )
        .build();

        // Apply schema adapter function - should pass through the source unchanged
        let result_source = apply_schema_adapter(source, &config);

        // Verify no adapter was applied
        assert!(result_source.schema_adapter_factory().is_none());
    }
}
