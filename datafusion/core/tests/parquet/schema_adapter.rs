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
            .with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory));

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
            .with_expr_adapter_factory(Arc::new(CustomPhysicalExprAdapterFactory));
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
            .with_expr_adapter_factory(Arc::new(CustomPhysicalExprAdapterFactory));
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
