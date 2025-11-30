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

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use bytes::{BufMut, BytesMut};
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
#[cfg(feature = "parquet")]
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::{
    ArrowSource, CsvSource, FileSource, JsonSource,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::config::CsvOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{ColumnStatistics, ScalarValue};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::schema_adapter::{
    SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::TableSchema;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;

async fn write_parquet(batch: RecordBatch, store: Arc<dyn ObjectStore>, path: &str) {
    let mut out = BytesMut::new().writer();
    {
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let data = out.into_inner().freeze();
    store.put(&Path::from(path), data.into()).await.unwrap();
}

/// A schema adapter factory that transforms column names to uppercase
#[derive(Debug, PartialEq)]
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
        let uppercase_name = field.name().to_uppercase();
        file_schema
            .fields()
            .iter()
            .position(|f| f.name().to_uppercase() == uppercase_name)
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::new();

        // Map each field in the table schema to the corresponding field in the file schema
        for table_field in self.table_schema.fields() {
            let uppercase_name = table_field.name().to_uppercase();
            if let Some(pos) = file_schema
                .fields()
                .iter()
                .position(|f| f.name().to_uppercase() == uppercase_name)
            {
                projection.push(pos);
            }
        }

        let mapper = UppercaseSchemaMapper {
            output_schema: self.output_schema(),
            projection: projection.clone(),
        };

        Ok((Arc::new(mapper), projection))
    }
}

impl UppercaseAdapter {
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

/// A physical expression adapter factory that maps uppercase column names to lowercase
#[derive(Debug)]
struct UppercasePhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for UppercasePhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(UppercasePhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
        })
    }
}

#[derive(Debug)]
struct UppercasePhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for UppercasePhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            if let Some(column) = e.as_any().downcast_ref::<Column>() {
                // Map uppercase column name (from logical schema) to lowercase (in physical file)
                let lowercase_name = column.name().to_lowercase();
                if let Ok(idx) = self.physical_file_schema.index_of(&lowercase_name) {
                    return Ok(Transformed::yes(
                        Arc::new(Column::new(&lowercase_name, idx))
                            as Arc<dyn PhysicalExpr>,
                    ));
                }
            }
            Ok(Transformed::no(e))
        })
        .data()
    }

    fn with_partition_values(
        &self,
        _partition_values: Vec<(FieldRef, ScalarValue)>,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(Self {
            logical_file_schema: self.logical_file_schema.clone(),
            physical_file_schema: self.physical_file_schema.clone(),
        })
    }
}

#[cfg(feature = "parquet")]
#[tokio::test]
async fn test_parquet_integration_with_physical_expr_adapter() -> Result<()> {
    // Create test data
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;

    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let store_url = ObjectStoreUrl::parse("memory://").unwrap();
    let path = "test.parquet";
    write_parquet(batch.clone(), store.clone(), path).await;

    // Get the actual file size from the object store
    let object_meta = store.head(&Path::from(path)).await?;
    let file_size = object_meta.size;

    // Create a session context and register the object store
    let ctx = SessionContext::new();
    ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));

    // Create a table schema with uppercase column names
    let table_schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("NAME", DataType::Utf8, true),
    ]));

    // Create a ParquetSource with the table schema (uppercase columns)
    let file_source = Arc::new(ParquetSource::new(table_schema.clone()));

    // Use PhysicalExprAdapterFactory to map uppercase column names to lowercase
    let config = FileScanConfigBuilder::new(store_url, file_source)
        .with_file(PartitionedFile::new(path, file_size))
        .with_expr_adapter(Some(Arc::new(UppercasePhysicalExprAdapterFactory)))
        .build();

    // Create a data source executor
    let exec = DataSourceExec::from_data_source(config);

    // Collect results
    let task_ctx = ctx.task_ctx();
    let stream = exec.execute(0, task_ctx)?;
    let batches = datafusion::physical_plan::common::collect(stream).await?;

    // There should be one batch
    assert_eq!(batches.len(), 1);

    // Verify the schema has the uppercase column names
    let result_schema = batches[0].schema();
    assert_eq!(result_schema.field(0).name(), "ID");
    assert_eq!(result_schema.field(1).name(), "NAME");

    // Verify the data was correctly read from the lowercase file columns
    // This confirms the PhysicalExprAdapter successfully mapped uppercase -> lowercase
    let id_array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .expect("Expected Int32Array for ID column");
    assert_eq!(id_array.values(), &[1, 2, 3]);

    let name_array = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("Expected StringArray for NAME column");
    assert_eq!(name_array.value(0), "a");
    assert_eq!(name_array.value(1), "b");
    assert_eq!(name_array.value(2), "c");

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

    // Test ArrowFileSource
    {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table_schema = TableSchema::new(schema, vec![]);
        let source = ArrowSource::new_file_source(table_schema);
        let source_with_adapter = source
            .clone()
            .with_schema_adapter_factory(factory.clone())
            .unwrap();

        let base_source: Arc<dyn FileSource> = source.into();
        assert!(base_source.schema_adapter_factory().is_none());
        assert!(source_with_adapter.schema_adapter_factory().is_some());

        let retrieved_factory = source_with_adapter.schema_adapter_factory().unwrap();
        assert_eq!(
            format!("{:?}", retrieved_factory.as_ref()),
            format!("{:?}", factory.as_ref())
        );
    }

    // Test CsvSource
    {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let options = CsvOptions {
            has_header: Some(true),
            delimiter: b',',
            quote: b'"',
            ..Default::default()
        };
        let source = CsvSource::new(schema).with_csv_options(options);
        let source_with_adapter = source
            .clone()
            .with_schema_adapter_factory(factory.clone())
            .unwrap();

        let base_source: Arc<dyn FileSource> = source.into();
        assert!(base_source.schema_adapter_factory().is_none());
        assert!(source_with_adapter.schema_adapter_factory().is_some());

        let retrieved_factory = source_with_adapter.schema_adapter_factory().unwrap();
        assert_eq!(
            format!("{:?}", retrieved_factory.as_ref()),
            format!("{:?}", factory.as_ref())
        );
    }

    // Test JsonSource
    {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table_schema = TableSchema::new(schema, vec![]);
        let source = JsonSource::new(table_schema);
        let source_with_adapter = source
            .clone()
            .with_schema_adapter_factory(factory.clone())
            .unwrap();

        let base_source: Arc<dyn FileSource> = source.into();
        assert!(base_source.schema_adapter_factory().is_none());
        assert!(source_with_adapter.schema_adapter_factory().is_some());

        let retrieved_factory = source_with_adapter.schema_adapter_factory().unwrap();
        assert_eq!(
            format!("{:?}", retrieved_factory.as_ref()),
            format!("{:?}", factory.as_ref())
        );
    }

    Ok(())
}
