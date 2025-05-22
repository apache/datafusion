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
use arrow::record_batch::RecordBatch;
use datafusion_common::{ColumnStatistics, DataFusionError, Result, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::impl_schema_adapter_methods;
use datafusion_datasource::schema_adapter::{
    SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// A test source for testing schema adapters
#[derive(Debug, Clone)]
struct TestSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl TestSource {
    fn new() -> Self {
        Self {
            schema_adapter_factory: None,
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
        unimplemented!("Not needed for this test")
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(Statistics::default())
    }

    impl_schema_adapter_methods!();
}

/// A test schema adapter factory
#[derive(Debug)]
struct TestSchemaAdapterFactory {}

impl SchemaAdapterFactory for TestSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(TestSchemaAdapter {
            table_schema: projected_table_schema,
        })
    }
}

/// A test schema adapter implementation
#[derive(Debug)]
struct TestSchemaAdapter {
    table_schema: SchemaRef,
}

impl SchemaAdapter for TestSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        file_schema.fields.find(field.name()).map(|(i, _)| i)
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        for (file_idx, file_field) in file_schema.fields().iter().enumerate() {
            if self.table_schema.fields().find(file_field.name()).is_some() {
                projection.push(file_idx);
            }
        }

        Ok((Arc::new(TestSchemaMapping {}), projection))
    }
}

/// A test schema mapper implementation
#[derive(Debug)]
struct TestSchemaMapping {}

impl SchemaMapper for TestSchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // For testing, just return the original batch
        Ok(batch)
    }

    fn map_column_statistics(
        &self,
        stats: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        // For testing, just return the input statistics
        Ok(stats.to_vec())
    }
}

#[test]
fn test_schema_adapter() {
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
    let source_with_adapter = source.with_schema_adapter_factory(factory);
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
