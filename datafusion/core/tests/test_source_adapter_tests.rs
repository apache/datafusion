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
use datafusion::physical_optimizer::filter_pushdown::util::TestSource;
use datafusion_common::Result;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory};
use std::sync::Arc;

/// A simple schema adapter factory for testing
#[derive(Debug)]
struct TestFilterPushdownAdapterFactory {}

impl SchemaAdapterFactory for TestFilterPushdownAdapterFactory {
    fn create(&self, schema: &Schema) -> Result<Box<dyn SchemaAdapter>> {
        Ok(Box::new(TestFilterPushdownAdapter {
            input_schema: Arc::new(schema.clone()),
        }))
    }
}

/// A simple schema adapter for testing
#[derive(Debug)]
struct TestFilterPushdownAdapter {
    input_schema: SchemaRef,
}

impl SchemaAdapter for TestFilterPushdownAdapter {
    fn adapt(&self, record_batch: arrow::record_batch::RecordBatch) -> Result<arrow::record_batch::RecordBatch> {
        Ok(record_batch)
    }

    fn output_schema(&self) -> SchemaRef {
        // Add a suffix to column names
        let fields = self
            .input_schema
            .fields()
            .iter()
            .map(|f| {
                Field::new(
                    format!("{}_modified", f.name()).as_str(),
                    f.data_type().clone(),
                    f.is_nullable(),
                )
            })
            .collect();

        Arc::new(Schema::new(fields))
    }
}

#[test]
fn test_test_source_schema_adapter_factory() {
    // Create a TestSource instance
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    
    let batches = vec![]; // Empty for this test
    let source = TestSource::new(true, batches);
    
    // Verify initial state has no adapter
    assert!(source.schema_adapter_factory().is_none());
    
    // Apply an adapter factory
    let factory = Arc::new(TestFilterPushdownAdapterFactory {});
    let source_with_adapter = source.with_schema_adapter_factory(factory);
    
    // Verify adapter was set
    assert!(source_with_adapter.schema_adapter_factory().is_some());
    
    // Create an adapter and validate the output schema
    let adapter_factory = source_with_adapter.schema_adapter_factory().unwrap();
    let adapter = adapter_factory.create(&schema).unwrap();
    let output_schema = adapter.output_schema();
    
    // Check modified column names
    assert_eq!(output_schema.field(0).name(), "id_modified");
    assert_eq!(output_schema.field(1).name(), "value_modified");
    
    // Check file type remains unchanged
    assert_eq!(source_with_adapter.file_type(), "test");
}

#[test]
fn test_test_source_default() {
    // Create a TestSource with default values for other fields
    let batches = vec![];
    let source = TestSource::new(false, batches);
    
    // Ensure schema_adapter_factory is None by default
    assert!(source.schema_adapter_factory().is_none());
}
