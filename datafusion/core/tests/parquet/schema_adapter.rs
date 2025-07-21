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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{record_batch, RecordBatch, RecordBatchOptions};
use arrow::compute::{cast_with_options, CastOptions};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use bytes::{BufMut, BytesMut};
use datafusion::assert_batches_eq;
use datafusion::common::Result;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::DataFusionError;
use datafusion_common::{ColumnStatistics, ScalarValue};
use datafusion_datasource::schema_adapter::{
    DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
};
use datafusion_datasource::ListingTableUrl;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::expressions::{self, Column};
use datafusion_physical_expr::schema_rewriter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion_physical_expr::{DefaultPhysicalExprAdapter, PhysicalExpr};
use itertools::Itertools;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;

#[cfg(feature = "parquet")]
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::{
    ArrowSource, CsvSource, FileOpener, FileScanConfig, FileScanConfigBuilder,
    FileSource, JsonSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion_datasource::PartitionedFile;

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

#[derive(Debug)]
struct CustomSchemaAdapterFactory;

impl SchemaAdapterFactory for CustomSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(CustomSchemaAdapter {
            logical_file_schema: projected_table_schema,
        })
    }
}

#[derive(Debug)]
struct CustomSchemaAdapter {
    logical_file_schema: SchemaRef,
}

impl SchemaAdapter for CustomSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        for (idx, field) in file_schema.fields().iter().enumerate() {
            if field.name() == self.logical_file_schema.field(index).name() {
                return Some(idx);
            }
        }
        None
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let projection = (0..file_schema.fields().len()).collect_vec();
        Ok((
            Arc::new(CustomSchemaMapper {
                logical_file_schema: Arc::clone(&self.logical_file_schema),
            }),
            projection,
        ))
    }
}

#[derive(Debug)]
struct CustomSchemaMapper {
    logical_file_schema: SchemaRef,
}

impl SchemaMapper for CustomSchemaMapper {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut output_columns =
            Vec::with_capacity(self.logical_file_schema.fields().len());
        for field in self.logical_file_schema.fields() {
            if let Some(array) = batch.column_by_name(field.name()) {
                output_columns.push(cast_with_options(
                    array,
                    field.data_type(),
                    &CastOptions::default(),
                )?);
            } else {
                // Create a new array with the default value for the field type
                let default_value = match field.data_type() {
                    DataType::Int64 => ScalarValue::Int64(Some(0)),
                    DataType::Utf8 => ScalarValue::Utf8(Some("a".to_string())),
                    _ => unimplemented!("Unsupported data type: {:?}", field.data_type()),
                };
                output_columns
                    .push(default_value.to_array_of_size(batch.num_rows()).unwrap());
            }
        }
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.logical_file_schema),
            output_columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .unwrap();
        Ok(batch)
    }

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        Ok(vec![
            ColumnStatistics::new_unknown();
            self.logical_file_schema.fields().len()
        ])
    }
}

// Implement a custom PhysicalExprAdapterFactory that fills in missing columns with the default value for the field type
#[derive(Debug)]
struct CustomPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for CustomPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(CustomPhysicalExprAdapter {
            logical_file_schema: Arc::clone(&logical_file_schema),
            physical_file_schema: Arc::clone(&physical_file_schema),
            inner: Arc::new(DefaultPhysicalExprAdapter::new(
                logical_file_schema,
                physical_file_schema,
            )),
        })
    }
}

#[derive(Debug, Clone)]
struct CustomPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    inner: Arc<dyn PhysicalExprAdapter>,
}

impl PhysicalExprAdapter for CustomPhysicalExprAdapter {
    fn rewrite(&self, mut expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr = expr
            .transform(|expr| {
                if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                    let field_name = column.name();
                    if self
                        .physical_file_schema
                        .field_with_name(field_name)
                        .ok()
                        .is_none()
                    {
                        let field = self
                            .logical_file_schema
                            .field_with_name(field_name)
                            .map_err(|_| {
                                DataFusionError::Plan(format!(
                                    "Field '{field_name}' not found in logical file schema",
                                ))
                            })?;
                        // If the field does not exist, create a default value expression
                        // Note that we use slightly different logic here to create a default value so that we can see different behavior in tests
                        let default_value = match field.data_type() {
                            DataType::Int64 => ScalarValue::Int64(Some(1)),
                            DataType::Utf8 => ScalarValue::Utf8(Some("b".to_string())),
                            _ => unimplemented!(
                                "Unsupported data type: {:?}",
                                field.data_type()
                            ),
                        };
                        return Ok(Transformed::yes(Arc::new(
                            expressions::Literal::new(default_value),
                        )));
                    }
                }

                Ok(Transformed::no(expr))
            })
            .data()?;
        self.inner.rewrite(expr)
    }

    fn with_partition_values(
        &self,
        partition_values: Vec<(FieldRef, ScalarValue)>,
    ) -> Arc<dyn PhysicalExprAdapter> {
        assert!(
            partition_values.is_empty(),
            "Partition values are not supported in this test"
        );
        Arc::new(self.clone())
    }
}

#[tokio::test]
async fn test_custom_schema_adapter_and_custom_expression_adapter() {
    let batch =
        record_batch!(("extra", Int64, [1, 2, 3]), ("c1", Int32, [1, 2, 3])).unwrap();

    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let store_url = ObjectStoreUrl::parse("memory://").unwrap();
    let path = "test.parquet";
    write_parquet(batch, store.clone(), path).await;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, true),
    ]));

    let mut cfg = SessionConfig::new()
        // Disable statistics collection for this test otherwise early pruning makes it hard to demonstrate data adaptation
        .with_collect_statistics(false)
        .with_parquet_pruning(false)
        .with_parquet_page_index_pruning(false);
    cfg.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));
    assert!(
        !ctx.state()
            .config_mut()
            .options_mut()
            .execution
            .collect_statistics
    );
    assert!(!ctx.state().config().collect_statistics());

    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_schema_adapter_factory(Arc::new(DefaultSchemaAdapterFactory))
            .with_physical_expr_adapter_factory(Arc::new(
                DefaultPhysicalExprAdapterFactory,
            ));

    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("SELECT c2, c1 FROM t WHERE c1 = 2 AND c2 IS NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = [
        "+----+----+",
        "| c2 | c1 |",
        "+----+----+",
        "|    | 2  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);

    // Test using a custom schema adapter and no explicit physical expr adapter
    // This should use the custom schema adapter both for projections and predicate pushdown
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_schema_adapter_factory(Arc::new(CustomSchemaAdapterFactory));
    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.deregister_table("t").unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("SELECT c2, c1 FROM t WHERE c1 = 2 AND c2 = 'a'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let expected = [
        "+----+----+",
        "| c2 | c1 |",
        "+----+----+",
        "| a  | 2  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);

    // Do the same test but with a custom physical expr adapter
    // Now the default schema adapter will be used for projections, but the custom physical expr adapter will be used for predicate pushdown
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_physical_expr_adapter_factory(Arc::new(
                CustomPhysicalExprAdapterFactory,
            ));
    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.deregister_table("t").unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("SELECT c2, c1 FROM t WHERE c1 = 2 AND c2 = 'b'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let expected = [
        "+----+----+",
        "| c2 | c1 |",
        "+----+----+",
        "|    | 2  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);

    // If we use both then the custom physical expr adapter will be used for predicate pushdown and the custom schema adapter will be used for projections
    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_schema_adapter_factory(Arc::new(CustomSchemaAdapterFactory))
            .with_physical_expr_adapter_factory(Arc::new(
                CustomPhysicalExprAdapterFactory,
            ));
    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.deregister_table("t").unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    let batches = ctx
        .sql("SELECT c2, c1 FROM t WHERE c1 = 2 AND c2 = 'b'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let expected = [
        "+----+----+",
        "| c2 | c1 |",
        "+----+----+",
        "| a  | 2  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);
}

// ----------------------------------------------------------------------
// Tests migrated from schema_adaptation/schema_adapter_integration_tests.rs
// ----------------------------------------------------------------------

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
            .position(|f| f.name() == field.name())
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let projection = (0..file_schema.fields().len()).collect::<Vec<_>>();

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

    // Create a ParquetSource with the adapter factory
    let file_source = ParquetSource::default()
        .with_schema_adapter_factory(Arc::new(UppercaseAdapterFactory {}))?;

    let config = FileScanConfigBuilder::new(store_url, batch.schema(), file_source)
        .with_file(PartitionedFile::new(path, file_size))
        .build();

    // Create a data source executor
    let exec = DataSourceExec::from_data_source(config);

    // Collect results
    let task_ctx = ctx.task_ctx();
    let stream = exec.execute(0, task_ctx)?;
    let batches = datafusion::physical_plan::common::collect(stream).await?;

    // There should be one batch
    assert_eq!(batches.len(), 1);

    // Verify the schema has the original column names (schema adapter not applied in DataSourceExec)
    let result_schema = batches[0].schema();
    assert_eq!(result_schema.field(0).name(), "ID");
    assert_eq!(result_schema.field(1).name(), "NAME");

    Ok(())
}

#[cfg(feature = "parquet")]
#[tokio::test]
async fn test_parquet_integration_with_schema_adapter_and_expression_rewriter(
) -> Result<()> {
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

    // Create a ParquetSource with the adapter factory
    let file_source = ParquetSource::default()
        .with_schema_adapter_factory(Arc::new(UppercaseAdapterFactory {}))?;

    let config = FileScanConfigBuilder::new(store_url, batch.schema(), file_source)
        .with_file(PartitionedFile::new(path, file_size))
        .build();

    // Create a data source executor
    let exec = DataSourceExec::from_data_source(config);

    // Collect results
    let task_ctx = ctx.task_ctx();
    let stream = exec.execute(0, task_ctx)?;
    let batches = datafusion::physical_plan::common::collect(stream).await?;

    // There should be one batch
    assert_eq!(batches.len(), 1);

    // Verify the schema has the original column names (schema adapter not applied in DataSourceExec)
    let result_schema = batches[0].schema();
    assert_eq!(result_schema.field(0).name(), "id");
    assert_eq!(result_schema.field(1).name(), "name");

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

    let arrow_source = ArrowSource::default();
    let arrow_source_with_adapter = ArrowSource::default()
        .with_schema_adapter_factory(factory.clone())
        .unwrap();
    assert!(arrow_source.schema_adapter_factory().is_none());
    // Verify adapters were properly set
    assert!(arrow_source_with_adapter.schema_adapter_factory().is_some());

    #[cfg(feature = "parquet")]
    let parquet_source = ParquetSource::default();
    #[cfg(feature = "parquet")]
    let parquet_source_with_adapter = ParquetSource::default()
        .with_schema_adapter_factory(factory.clone())
        .unwrap();
    #[cfg(feature = "parquet")]
    assert!(parquet_source.schema_adapter_factory().is_none());
    #[cfg(feature = "parquet")]
    assert!(parquet_source_with_adapter
        .schema_adapter_factory()
        .is_some());

    let csv_source = CsvSource::default();
    let csv_source_with_adapter = CsvSource::default()
        .with_schema_adapter_factory(factory.clone())
        .unwrap();
    assert!(csv_source.schema_adapter_factory().is_none());
    assert!(csv_source_with_adapter.schema_adapter_factory().is_some());

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
