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
use datafusion_common::{ColumnStatistics, DataFusionError, Result, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::schema_adapter::{
    SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType};
use object_store::ObjectStore;
use std::fmt::Debug;
use std::sync::Arc;

// Simple TestSource implementation for testing without dependency on private module
#[derive(Clone, Debug)]
struct TestSource {
    #[allow(dead_code)]
    has_adapter: bool,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl TestSource {
    fn new(has_adapter: bool) -> Self {
        Self {
            has_adapter,
            schema_adapter_factory: None,
        }
    }
}

impl FileSource for TestSource {
    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Arc<dyn FileSource> {
        Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        })
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn file_type(&self) -> &str {
        "test"
    }

    fn as_any(&self) -> &dyn std::any::Any {
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
}

impl DisplayAs for TestSource {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "TestSource")
            }
        }
    }
}

/// A simple schema adapter factory for testing
#[derive(Debug)]
struct TestFilterPushdownAdapterFactory {}

impl SchemaAdapterFactory for TestFilterPushdownAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(TestFilterPushdownAdapter {
            input_schema: projected_table_schema,
        })
    }
}

/// A simple schema adapter for testing
#[derive(Debug)]
struct TestFilterPushdownAdapter {
    input_schema: SchemaRef,
}

impl SchemaAdapter for TestFilterPushdownAdapter {
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

        // Create a schema mapper that modifies column names
        #[derive(Debug)]
        struct TestSchemaMapping {
            #[allow(dead_code)]
            input_schema: SchemaRef,
        }

        impl SchemaMapper for TestSchemaMapping {
            fn map_batch(
                &self,
                batch: arrow::record_batch::RecordBatch,
            ) -> Result<arrow::record_batch::RecordBatch> {
                // For testing, just return the original batch
                Ok(batch)
            }

            fn map_column_statistics(
                &self,
                file_col_statistics: &[ColumnStatistics],
            ) -> Result<Vec<ColumnStatistics>> {
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

#[test]
fn test_test_source_schema_adapter_factory() {
    // Create a TestSource instance
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));

    let source = TestSource::new(true);

    // Verify initial state has no adapter
    assert!(source.schema_adapter_factory().is_none());

    // Apply an adapter factory
    let factory = Arc::new(TestFilterPushdownAdapterFactory {});
    let source_with_adapter = source.with_schema_adapter_factory(factory);

    // Verify adapter was set
    assert!(source_with_adapter.schema_adapter_factory().is_some());

    // Create an adapter
    let adapter_factory = source_with_adapter.schema_adapter_factory().unwrap();
    let adapter = adapter_factory.create(Arc::clone(&schema), Arc::clone(&schema));

    // Create a file schema to test mapping
    let file_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]);

    // Test column mapping
    let id_index = adapter.map_column_index(0, &file_schema);
    assert_eq!(id_index, Some(0));

    // Test schema mapping
    let (_mapper, projection) = adapter.map_schema(&file_schema).unwrap();
    assert_eq!(projection.len(), 2); // Both columns should be included

    // Check file type remains unchanged
    assert_eq!(source_with_adapter.file_type(), "test");
}

#[test]
fn test_test_source_default() {
    // Create a TestSource with default values
    let source = TestSource::new(false);

    // Ensure schema_adapter_factory is None by default
    assert!(source.schema_adapter_factory().is_none());
}
